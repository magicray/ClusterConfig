import time
from distutils.core import setup

setup(
  name='PaxosDB',
  scripts=['paxosdb'],
  py_modules=['paxosdb'],
  version=time.strftime('%Y%m%d'),
  install_requires=['http-rpc'],
  description='Highly Available key value store with atomic updates - '
              'Replicated and Strongly Consistent',
  long_description='Leaderless. '
                   'Paxos for synchronous and consistent replication. '
                   'SQLite for persistence. '
                   'HTTPs interface.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/PaxosDB',
  keywords=['paxos', 'consistent', 'replicated', 'cluster', 'config']
)
