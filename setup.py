
# Use this command for deploy.
#   python3 setup.py sdist bdist_wheel && python3 -m twine upload --skip-existing dist/*

import io
from setuptools import find_packages, setup

setup(name='data_warehouse',
      version='1.0.0',
      description='데이터 웨어하우스를 만들 때 사용하는 간소화된 인터페이스입니다.',
      long_description="Please refer to the https://github.com/da-huin/data_warehouse",
      long_description_content_type="text/markdown",
      url='https://github.com/da-huin/data_warehouse',
      download_url= 'https://github.com/da-huin/data_warehouse/archive/master.zip',
      author='JunYeong Park',
      author_email='dahuin000@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[],
      classifiers=[
          'Programming Language :: Python :: 3',
    ]
)


