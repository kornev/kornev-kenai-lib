.PHONY: all clean test style jar install deploy repl
.SILENT:    clean test style jar install deploy repl

.DEFAULT: all

all: clean test

clean:
	if [ -d "target"                                 ]; then rm --recursive target;                  fi
	if [ `ls -a apps/hive             | wc -l` -gt 3 ]; then rm --recursive apps/hive/*;             fi
	if [ `ls -a apps/spark/warehouse  | wc -l` -gt 3 ]; then rm --recursive apps/spark/warehouse/*;  fi
	if [ `ls -a apps/spark/checkpoint | wc -l` -gt 3 ]; then rm --recursive apps/spark/checkpoint/*; fi

test:
	lein with-profile +spark-2.4 cloverage

style:
	lein cljfmt fix

jar:
	lein with-profile +spark-2.4 jar

install:
	lein with-profile +spark-2.4 install

repl:
	lein with-profile +spark-2.4 repl
