import sys
import os

sys.path.insert(0, os.path.abspath('..'))

# Pull the version string out of setup.py without importing it
with open('../setup.py') as f:
    for line in f:
        if 'VERSION' in line:
            __version__ = eval(line.split('=')[1])
            break

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
]

master_doc = 'index'
project = u'zprocess'
copyright = u'2013-2018, Chris Billington'
version = __version__
release = '.'.join(__version__.split('.')[:-1])

autodoc_member_order = 'bysource'
autoclass_content = 'both'

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:  # only import and set the theme if we're building docs locally
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# # Monkeypatch add_directive_header method of AttributeDocumenter to not show
# # values of attributes, autodoc doesn't seem to be able to find them anyway
# # for our enums - they all come out as None.
# from sphinx.ext.autodoc import AttributeDocumenter, ClassLevelDocumenter

# def add_directive_header(self, sig):
#     ClassLevelDocumenter.add_directive_header(self, sig)

# AttributeDocumenter.add_directive_header = add_directive_header