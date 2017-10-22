#!/usr/bin/env python3

extensions = []

templates_path = ['_templates']

source_suffix = '.rst'

master_doc = 'index'

project = 'asgi_rabbitmq'
copyright = '2017, Django Software Foundation'
author = 'Django Software Foundation'

version = '0.5.4'
release = '0.5.4'

language = None

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

pygments_style = 'sphinx'

todo_include_todos = False

html_theme = 'alabaster'

html_static_path = ['_static']

htmlhelp_basename = 'asgi_rabbitmqdoc'

latex_elements = {}

latex_documents = [
    (master_doc, 'asgi_rabbitmq.tex', 'asgi\\_rabbitmq Documentation',
     'Django Software Foundation', 'manual'),
]

man_pages = [
    (master_doc, 'asgi_rabbitmq', 'asgi_rabbitmq Documentation',
     [author], 1),
]

texinfo_documents = [
    (master_doc, 'asgi_rabbitmq', 'asgi_rabbitmq Documentation', author,
     'asgi_rabbitmq', 'One line description of project.', 'Miscellaneous'),
]
