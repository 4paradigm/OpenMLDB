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

## Tips

 Here are some important tips to help you get started:

- For any changes, you must commit them before building. Changes that are not committed will not be reflected in the generated HTML files.
- We are using MyST parser to support Markdown in Sphinx. Furthermore, MyST has a few very useful syntax extensions for better formatting. You may read [the syntax guide of MyST](https://myst-parser.readthedocs.io/en/latest/syntax/syntax.html).

## Deployment

For our docs website ([https://openmldb.ai/docs/zh/](https://openmldb.ai/docs/zh/) and [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/main/)), the building and deployment are automatically triggered by any push events to the `docs` of this repo. The deployment script currently is maintained in a private repo, if you experience any problems, please contact the repo maintainers. 
