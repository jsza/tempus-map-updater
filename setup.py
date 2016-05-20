from setuptools import find_packages, setup



setup(
    name="mapupdater",
    packages=find_packages() + ["twisted.plugins"],
    install_requires=[
        "autobahn == 0.13.0",
        "twisted >= 15.0.0",
        "treq",
        "service_identity >= 14.0.0"
    ]
)
