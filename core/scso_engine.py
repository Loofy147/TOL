import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
import collections
import time
import logging

log = logging.getLogger("scso_engine")

class SCSORouter:
    def __init__(self, alpha=0.7, beta=0.3):
        self.alpha = alpha
        self.beta = beta
        self.skills = {}  # id -> {"corpus": [str], "cost": float}
        self.vectorizer = TfidfVectorizer()
        self.tfidf_matrix = None
        self.kmeans = None
        self.skill_ids = []

    def register_skill(self, skill_id, corpus, initial_cost=0.1):
        self.skills[skill_id] = {"corpus": corpus, "cost": initial_cost}
        self.skill_ids = list(self.skills.keys())

    def initialize_topology(self, k=2):
        if not self.skills:
            return

        all_text = []
        skill_indices = []
        for idx, skill_id in enumerate(self.skill_ids):
            corpus = self.skills[skill_id]["corpus"]
            all_text.extend(corpus)
            skill_indices.extend([idx] * len(corpus))

        self.tfidf_matrix = self.vectorizer.fit_transform(all_text)

        num_samples = self.tfidf_matrix.shape[0]
        actual_k = min(k, num_samples)
        if actual_k > 1:
            self.kmeans = KMeans(n_clusters=actual_k, n_init=10, random_state=42)
            self.kmeans.fit(self.tfidf_matrix)
        else:
            self.kmeans = None

    def predict_next(self, context):
        if not self.skills or self.tfidf_matrix is None:
            return None, 0.0, None

        context_vec = self.vectorizer.transform([context])

        best_skill = None
        max_utility = -float('inf')

        for skill_id in self.skill_ids:
            # Semantic Similarity (Max cosine similarity between context and any doc in skill corpus)
            skill_docs = self.skills[skill_id]["corpus"]
            skill_vecs = self.vectorizer.transform(skill_docs)
            similarities = cosine_similarity(context_vec, skill_vecs)[0]
            max_sim = np.max(similarities)

            # Utility Model: U = alpha * Similarity - beta * Cost
            cost = self.skills[skill_id]["cost"]
            utility = self.alpha * max_sim - self.beta * cost

            if utility > max_utility:
                max_utility = utility
                best_skill = skill_id

        return best_skill, max_utility, context_vec

class SCSOEngine:
    def __init__(self, max_instances=5, initial_threshold=0.2, alpha=0.7, beta=0.3):
        self.router = SCSORouter(alpha=alpha, beta=beta)
        self.max_instances = max_instances
        self.prefetch_threshold = initial_threshold

        self.skill_registry = {}  # id -> skill_class
        self._instances = collections.OrderedDict()  # LRU: id -> instance

        self.execution_times = collections.defaultdict(list) # id -> [times]
        self.outcome_window = collections.deque(maxlen=20) # [bool] (hits/misses)

    def register_skill(self, skill_id, corpus, skill_class, initial_cost=0.1):
        self.router.register_skill(skill_id, corpus, initial_cost)
        self.skill_registry[skill_id] = skill_class

    def initialize_topology(self, k=2):
        self.router.initialize_topology(k)

    def process_request(self, skill_id, context):
        # 1. Prediction & Potential Pre-fetch
        pred_skill, utility, context_vec = self.router.predict_next(context)

        if pred_skill and utility > self.prefetch_threshold:
            if pred_skill not in self._instances:
                # Speculative pre-fetch
                self._load_instance(pred_skill)

        # 2. Execution
        start_time = time.time()
        instance = self._get_instance(skill_id)
        if instance:
            # Note: The actual call happens in the gateway.
            # We just return the instance here.
            duration = time.time() - start_time # Just the "loading" part
            self.record_execution_time(skill_id, duration)
            self.outcome_window.append(True)
        else:
            self.outcome_window.append(False)

        # 3. Dynamic Threshold Tuning (Simple heuristic)
        if len(self.outcome_window) >= 10:
            hit_rate = sum(self.outcome_window) / len(self.outcome_window)
            if hit_rate < 0.5:
                self.prefetch_threshold *= 0.9 # Aggressive
            elif hit_rate > 0.9:
                self.prefetch_threshold *= 1.1 # Conservative

        return instance

    def _get_instance(self, skill_id):
        if skill_id in self._instances:
            self._instances.move_to_end(skill_id)
            return self._instances[skill_id]

        return self._load_instance(skill_id)

    def _load_instance(self, skill_id):
        if skill_id not in self.skill_registry:
            return None

        if len(self._instances) >= self.max_instances:
            # LRU Eviction
            self._instances.popitem(last=False)

        skill_class = self.skill_registry[skill_id]
        instance = skill_class()
        self._instances[skill_id] = instance
        return instance

    def record_execution_time(self, skill_id, duration):
        # EWMA update for cost
        gamma = 0.2
        current_cost = self.router.skills[skill_id]["cost"]
        new_cost = (1 - gamma) * current_cost + gamma * duration
        self.router.skills[skill_id]["cost"] = new_cost
