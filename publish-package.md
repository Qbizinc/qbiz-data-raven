Follow directions at [Packaging Python Projects](https://packaging.python.org/tutorials/packaging-projects/)
Build the package using the _setup_ package, which should already be installed in your environment.
```
python3 setup.py sdist bdist_wheel
```
To push a package to _pypi_, you must first install _twine_.  You will be asked to authenticate.  You will need to use one of the account authentication tokens.
```
python3 -m pip install --user --upgrade twine
python3 -m twine upload --verbose --repository testpypi dist/*

SorenMacBookAir:data-raven sorenarchibald$ python3 -m twine upload --verbose --repository testpypi dist/*
Uploading distributions to https://test.pypi.org/legacy/
  dist/qbiz_data_raven-1.0.1-py3-none-any.whl (31.0 KB)
  dist/qbiz-data-raven-1.0.1.tar.gz (14.0 KB)
Enter your username: __token__
Enter your password:
Uploading qbiz_data_raven-1.0.1-py3-none-any.whl
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 42.2k/42.2k [00:01<00:00, 27.3kB/s]
Uploading qbiz-data-raven-1.0.1.tar.gz
100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 25.2k/25.2k [00:00<00:00, 26.4kB/s]

View at:
https://test.pypi.org/project/qbiz-data-raven/1.0.1/
```
