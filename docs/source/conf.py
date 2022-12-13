# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys

import toml

sys.path.insert(0, os.path.abspath("../.."))

project = "cognite-cdffs"
copyright = "2022, Cognite AS"
author = "Infant.Alex@cognite.com"

# Get release from pyproject.toml file
release = toml.load("./../../pyproject.toml")["tool"]["poetry"]["version"]

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx_rtd_theme", "sphinx.ext.autosummary", "sphinx.ext.autodoc"]

exclude_patterns = []
autosummary_generate = True
autodoc_member_order = "bysource"

# # -- Options for HTML output -------------------------------------------------
# # https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
