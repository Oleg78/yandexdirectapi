from setuptools import setup

setup(name='yandexdirectapi',
      version='0.1',
      description='Yandex Direct API',
      url='https://github.com/Oleg78/yandexdirectapi.git',
      author='Oleg78',
      author_email='olegaleksandrovich@ya.ru',
      license='MIT',
      packages=['yandexdirectapi'],
      zip_safe=False, install_requires=['requests', 'aiohttp'])