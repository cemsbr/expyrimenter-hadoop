dev_pep8:
	@find . -name "*.py" -type f \
	    | grep -v "^./doc/" | grep -v "^./ipython/" | xargs pep8

dev_clean:
	git clean -dxf
