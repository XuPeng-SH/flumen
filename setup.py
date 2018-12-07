from setuptools import setup

setup(
    name='flumen',
    version='0.1',
    description=(
        'Event stream framework'
    ),
    long_description=open('README.md').read(),
    author='Xu Peng',
    author_email='xupeng3112@163.com',
    url='https://github.com/pangyun/flumen',
    packages=['flumen', 'demo'],
    include_package_data=True
)
