# required files for scraping
links := shopify.txt
scraper := shopify/shopify.py

# filter files to be moved to data dir
pattern_inc := (product|offers).*\.(csv|json)
pattern_exc := [0-9]{4}-[0-9]{2}-[0-9]{2}

all: data

data: process
	for file in $$(ls | grep -E "${pattern_inc}" | grep -Ev "${pattern_exc}"); do \
	file_rename_to=data/$${file%.*}_$$(date +"%FT%H:%M%z").$${file##*.}; \
	mv $${file} $${file_rename_to}; \
	echo "Saved data to $${file_rename_to}"; \
	done

process: $(scraper) $(links)
	for url in $$(cat ${links} | grep .); do \
	python ${scraper} -p p.csv -o o.csv -t $$url; \
	tail -n +2 p.csv >> products0.csv; \
	tail -n +2 o.csv >> offers0.csv; \
	done; \
	head -n 1 p.csv > pfields.csv; \
	head -n 1 o.csv > ofields.csv; \
	cat pfields.csv products0.csv > products.csv; \
	cat ofields.csv offers0.csv > offers.csv

$(scraper):
	[ -f ${scraper} ] && echo "Using ${scraper}" || exit 1

$(links):
	[ -f ${links} ] && echo "Using links in ${links}" || exit 1

clean:
	rm *.csv

.PHONY: all data process $(scraper) $(links) clean
