class IncorpApiMixin:
    # ## Set Retry Controls
    # @classmethod
    # def retryREST(session, retryUrl, backoffFactor=1.0):
    #     retryControls = Retry(
    #         total=5,
    #         backoff_factor=backoffFactor,
    #         status_forcelist=[429, 500, 502, 503, 504],
    #     )
    #     session.mount("https://", HTTPAdapter(max_retries=retryControls))
    #     return session.get(retryUrl)
    pass


