import time
from distutils.core import setup

setup(
  name='ConfDB',
  scripts=['confdb'],
  py_modules=['confdb'],
  version=time.strftime('%Y%m%d'),
  description='Highly Available key value store with atomic updates - '
              'Replicated and Strongly Consistent',
  long_description='Leaderless. '
                   'Paxos for synchronous and consistent replication. '
                   'SQLite for persistence. '
                   'HTTPs interface.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/ConfDB',
  keywords=['paxos', 'consistent', 'replicated', 'cluster', 'config']
)
