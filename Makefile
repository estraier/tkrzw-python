# Makefile for Tkrzw for Python

PACKAGE = tkrzw-python
VERSION = 0.1.11
PACKAGEDIR = $(PACKAGE)-$(VERSION)
PACKAGETGZ = $(PACKAGE)-$(VERSION).tar.gz

PYTHON = python3
RUNENV = LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib

all :
	$(PYTHON) setup.py build
	cp -f build/*/*.so .
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Ready to install.\n'
	@printf '#================================================================\n'

clean :
	rm -rf casket casket* *~ *.tmp *.tkh *.tkt *.tks *.so *.pyc build hoge moge tako ika

install :
	$(PYTHON) setup.py install
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Thanks for using Tkrzw for Python.\n'
	@printf '#================================================================\n'

uninstall :
	$(PYTHON) setup.py install --record files.tmp
	xargs rm -f < files.tmp

dist :
	$(MAKE) distclean
	rm -Rf "../$(PACKAGEDIR)" "../$(PACKAGETGZ)"
	cd .. && cp -R tkrzw-python $(PACKAGEDIR) && \
	  tar --exclude=".*" -cvf - $(PACKAGEDIR) | gzip -c > $(PACKAGETGZ)
	rm -Rf "../$(PACKAGEDIR)"
	sync ; sync

distclean : clean apidocclean

check :
	$(RUNENV) $(PYTHON) test.py 
	$(RUNENV) $(PYTHON) perf.py --path casket.tkh --params "num_buckets=100000" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) perf.py --path casket.tkh --params "concurrent=true,num_buckets=100000" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --path casket.tkt --params "key_comparator=decimal" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) perf.py --path casket.tkt --params "concurrent=true,key_comparator=decimal" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --path casket.tks --params "step_unit=3" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) perf.py --path casket.tks --params "concurrent=true,step_unit=3" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --params "dbm=tiny,num_buckets=100000" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --params "dbm=baby,key_comparator=decimal" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --params "dbm=stdhash,num_buckets=100000" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) perf.py --params "dbm=stdtree" \
	  --iter 20000 --threads 5 --random
	$(RUNENV) $(PYTHON) wicked.py --path casket.tkh --params "num_buckets=100000" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) wicked.py --path casket.tkt --params "key_comparator=decimal" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) wicked.py --path casket.tks --params "step_unit=3" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) wicked.py --params "dbm=tiny,num_buckets=100000" \
	  --iter 20000 --threads 5
	$(RUNENV) $(PYTHON) wicked.py --params "dbm=baby,key_comparator=decimal" \
	  --iter 20000 --threads 5
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Checking completed.\n'
	@printf '#================================================================\n'

apidoc :
	$(MAKE) apidocclean
	mkdir -p tmp-doc
	cp tkrzw-doc.py tmp-doc/tkrzw.py
	cd tmp-doc ; sphinx-apidoc -F -H Tkrzw -A "Mikio Hirabayashi" -o out .
	cat tmp-doc/out/conf.py |\
	  sed -e 's/^# import /import /' -e 's/^# sys.path/sys.path/' \
	    -e 's/alabaster/haiku/' \
      -e '/sphinx\.ext\.viewcode/d' \
      -e '/^extensions = /a "sphinx.ext.autosummary",' > tmp-doc/out/conf.py.tmp
	echo >> tmp-doc/out/conf.py.tmp
	echo "autodoc_member_order = 'bysource'" >> tmp-doc/out/conf.py.tmp
	echo "html_title = 'Python binding of Tkrzw'" >> tmp-doc/out/conf.py.tmp
	mv -f tmp-doc/out/conf.py.tmp tmp-doc/out/conf.py
	cp -f tkrzw-index.rst tmp-doc/out/index.rst
	cd tmp-doc/out ; $(MAKE) html
	mv tmp-doc/out/_build/html api-doc

apidocclean :
	rm -rf api-doc tmp-doc

.PHONY: all clean install uninstall dist distclean check apidoc apidocclean

# END OF FILE
