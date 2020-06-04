
all: install clean
	
install: 
	@echo 
	@echo "Install gjsonc extension"
	@echo "=================="
	@echo
	
	cat inc/tmpl.i                            >  gjsonc.i 

	cat inc/gjsonc.i                          >> gjsonc.i 
	
	echo "%pythoncode %{"                     >> gjsonc.i

	cat inc/geojsoner.py                      >> gjsonc.i 

	echo "%}"                                 >> gjsonc.i
	
	# -builtin -modern -modernargs 
	swig -c++ -python -O -py3 gjsonc.i
	
	$(RM) -rf  /usr/lib64/python3.6/site-packages/gjsonc*
	$(RM) -rf  /usr/lib/python3.6/site-packages/gjsonc* 
	$(RM) -rf  /usr/local/lib/python3.6/site-packages/gjsonc*
	
	$(RM) -rf  /usr/lib64/python3.7/site-packages/gjsonc*
	$(RM) -rf  /usr/lib/python3.7/site-packages/gjsonc* 
	$(RM) -rf  /usr/local/lib/python3.7/site-packages/gjsonc*
	
	python3 setup.py install --prefix=/usr
	
uninstall: 	
	@echo
	@echo "Cleaning installed files"
	@echo "=================="
	@echo
	
	$(RM) -rf  /usr/lib64/python3.6/site-packages/gjsonc*
	$(RM) -rf  /usr/lib/python3.6/site-packages/gjsonc* 
	$(RM) -rf  /usr/local/lib/python3.6/site-packages/gjsonc* 
    
	$(RM) -rf  /usr/lib64/python3.7/site-packages/gjsonc*
	$(RM) -rf  /usr/lib/python3.7/site-packages/gjsonc* 
	$(RM) -rf  /usr/local/lib/python3.7/site-packages/gjsonc*
    
clean: 
	@echo
	@echo "Cleaning cached files"
	@echo "=================="
	@echo
	
	$(RM) -rf build dist gjsonc.egg-info __pycache__
		