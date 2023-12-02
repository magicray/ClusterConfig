import time
from distutils.core import setup

setup(
  name='ConfDB',
  packages=['confdb'],
  version=time.strftime('%Y%m%d'),
  description='Highly Available store for configuration variables - '
              'Replicated and Strongly Consistent',
  long_description='Leaderless. '
                   'Paxos for synchronous and consistent replication. '
                   'SQLite for persistence. HTTP/mTLS interface.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/ConfDB',
  keywords=['paxos', 'consistent', 'replicated', 'config']
)
