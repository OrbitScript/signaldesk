from setuptools import setup, find_packages
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()
setup(
    name="signaldesk",
    version="1.0.0",
    description="Work Signal Engine — converts noise into actionable insights",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OrbitScript/signaldesk",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[],
    extras_require={"dev": ["pytest>=7.0"]},
    entry_points={"console_scripts": ["signaldesk=signaldesk.cli:main"]},
)
