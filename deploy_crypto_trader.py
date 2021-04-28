import ray
from ray import serve
from ppo_crypto_backend import CryptoTradingModel

ray.init(address="auto", ignore_reinit_error=True)

client = serve.start(detached=True)

client.create_backend("ppo_crypto_backend", CryptoTradingModel)
client.create_endpoint("ppo_endpoint", backend="ppo_crypto_backend", route="/trade_crypto")