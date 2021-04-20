DOXYBOOK2=$1
export LC_CTYPE=C &&  export LANG=C && grep -rl "kind=\"enum\"" xml |xargs sed -i "" 's/kind=\"enum\"/kind=\"class\"/g'
export LC_CTYPE=C &&  export LANG=C && grep -rl "::" xml |xargs sed -i "" 's/::/./g'
$DOXYBOOK2 --input xml --output java --config config.json --templates ../template --summary-input SUMMARY.md.tmpl --summary-output SUMMARY.md

