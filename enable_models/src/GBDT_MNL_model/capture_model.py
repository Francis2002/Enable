# capture_model.py

import numpy as np
from scipy.optimize import minimize
from scipy.special import softmax, logsumexp


class WeightedConditionalLogit:
    def __init__(self, l2=1e-4, maxiter=500):
        self.l2 = l2
        self.maxiter = maxiter
        self.beta_ = None
        self.opt_result_ = None

    def initialize(self, beta0: np.ndarray):
        self.beta_ = np.asarray(beta0, dtype=np.float64).copy()
        return self

    def _loss_and_grad(self, beta, Z, F):
        # Z: (C, K, R)
        # F: (C, K)

        U = np.einsum("ckr,r->ck", Z, beta)  # (C, K)
        P = softmax(U, axis=1)               # (C, K)

        log_denom = logsumexp(U, axis=1)     # (C,)
        loss = -np.sum(F * (U - log_denom[:, None]))
        loss += 0.5 * self.l2 * np.sum(beta ** 2)

        m = F.sum(axis=1, keepdims=True)     # (C, 1)
        residual = m * P - F                 # (C, K)
        grad = np.einsum("ck,ckr->r", residual, Z)
        grad += self.l2 * beta

        return loss, grad

    def fit(self, Z, F, beta0=None):
        R = Z.shape[2]

        if beta0 is None:
            if self.beta_ is None:
                beta0 = np.zeros(R, dtype=np.float64)
            else:
                beta0 = self.beta_

        beta0 = np.asarray(beta0, dtype=np.float64)

        res = minimize(
            fun=lambda b: self._loss_and_grad(b, Z, F),
            x0=beta0,
            method="L-BFGS-B",
            jac=True,
            options={"maxiter": self.maxiter},
        )

        self.beta_ = res.x
        self.opt_result_ = res
        return self

    def predict_proba(self, Z):
        if self.beta_ is None:
            raise ValueError(
                "WeightedConditionalLogit is not initialized/fitted. "
                "Call capture_model.initialize(beta0) or capture_model.fit(...)."
            )

        U = np.einsum("ckr,r->ck", Z, self.beta_)
        P = softmax(U, axis=1)
        return P