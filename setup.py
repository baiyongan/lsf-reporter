# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(name='lsf-reporter',
      version='0.1',
      description='the lsf reporter',
      url='http://gitlab.xtc.home/xtc/lsf-reporter',
      author='baiyongan',
      author_email='baiyongan@compubiq.com',
      license='MIT',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      zip_safe=False,
      install_requires=[
          "elasticsearch == 7.5.1",
          "python-consul == 1.1.0",
          "PyYAML == 5.3.1",
          "Flask==1.1.1",
          "Flask-Cors==3.0.8",
          "openpyxl==3.0.3"
      ],
      entry_points={
          'console_scripts': [
              # 'lsf-reporter=lsf_reporter:main',
          ],
      },
      )
