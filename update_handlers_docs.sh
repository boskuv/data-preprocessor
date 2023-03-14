cd docs_generator
make markdown;

cd ..
cd ..

rm app/processing/handlers/README.md;
cat docs_generator/_build/markdown/app.processing.handlers.md >> README.md;