import time
from distutils.core import setup

setup(
  name='ClusterConfig',
  packages=['cluster_config'],
  version=time.strftime('%Y%m%d'),
  description='Highly Available key value store for atomic updates - '
              'Replicated and Strongly Consistent',
  long_description='Leaderless. '
                   'Paxos for synchronous and consistent replication. '
                   'SQLite for persistence. HTTP/mTLS interface.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/ClusterConfig',
  keywords=['paxos', 'consistent', 'replicated', 'cluster', 'config']
)
