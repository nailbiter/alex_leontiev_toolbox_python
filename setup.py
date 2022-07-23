# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['alex_leontiev_toolbox_python']

package_data = \
{'': ['*']}

setup_kwargs = {
    'name': 'alex-leontiev-toolbox-python',
    'version': '0.1.0',
    'description': '',
    'long_description': None,
    'author': 'Oleksii Leontiev',
    'author_email': 'alozz1991@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'python_requires': '>=3.9,<4.0',
}


setup(**setup_kwargs)
