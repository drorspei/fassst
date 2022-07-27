from setuptools import setup


if __name__ == "__main__":
    setup(
        name="cps3",
        version="1.0.1",
        description="Fast list/copy for s3 and in general",
        long_description=open("README.md").read(),
        author="Dror Speiser",
        url="https://github.com/drorspei/fassst",
        license="MIT",
        classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
        ],
        packages=["cps3"],
        package_dir={'': 'python/'},
        python_requires=">=3.8",
        install_requires=["fsspec"],
        entry_points={
            'console_scripts': [
                'cps3 = cps3.main:main',                  
            ],              
        },
    )
