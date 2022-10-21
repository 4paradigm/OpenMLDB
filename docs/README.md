# README

This is the source code of documentations, you may visit our website for smooth reading: 

- English: [https://openmldb.ai/docs/en/main/](https://openmldb.ai/docs/en/main/)
- Chinese: [https://openmldb.ai/docs/zh](https://openmldb.ai/docs/zh)

## Build

We are using [Sphinx](https://www.sphinx-doc.org/) together with the [sphinx-multiversion](https://holzhaus.github.io/sphinx-multiversion), [MyST](https://myst-parser.readthedocs.io/), and [book theme](https://sphinx-book-theme.readthedocs.io/) to build the documentations. You may follow the below steps to build locally:

1. You are suggested to use Conda to setup the toolchain
2. Creating a new conda environment by importing the environment file
   ```bash
   conda env create -f environment.yml
   conda activate sphinx
   ```
3. Building the Chinese or English doc
   ```bash
   sphinx-multiversion en build/en # the English documentations
   sphinx-multiversion zh build/zh # the Chinese documentations
   ```
### Note
After activation, you can use `which python` to check. 

If python is not from conda envs 'sphinx', you may activate 'sphinx' from the (base) env.

Deactivate `(base)` env, and then `conda activate sphinx`.

If you fail to create conda envs by `environment.yml`, try following commands.
   ```bash
   1. conda create -n sphinx 
   2. conda activate sphinx 
   3. pip3 install -U Sphinx 
   4. pip3 install sphinx-multiversion 
   5. pip3 install myst-parser
   6. pip3 install sphinx-book-theme
   7. pip3 install sphinx-copybutton
   8. pip3 install myst-parser[linkify]
   ```
## Tips

 Here are some important tips to help you get started:

- For any changes, you must commit them before building. Changes that are not committed will not be reflected in the generated HTML files.
- We just build the branches which match `smv_branch_whitelist=r"^(main|v\d+\.\d+)$"`. If you want to build your own branch, e.g. my-branch, you can set `smv_branch_whitelist=r"^(main|v\d+\.\d+|my-branch)$` temporarily.
- We are using MyST parser to support Markdown in Sphinx. Furthermore, MyST has a few very useful syntax extensions for better formatting. You may read [the syntax guide of MyST](https://myst-parser.readthedocs.io/en/latest/syntax/syntax.html).

## Deployment

For our docs website ([https://openmldb.ai/docs/zh/](https://openmldb.ai/docs/zh/) and [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/main/)), the building and deployment are automatically triggered by any push events to the `docs` of this repo. The deployment script currently is maintained in a private repo, if you experience any problems, please contact the repo maintainers. 
