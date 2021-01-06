all: build

build: clean
	mkdir -p bin
	docker build -t re_dms .
	docker create -ti --name re_dms re_dms bash
	docker cp re_dms:/tmp/re_dms/target/release/re_dms ./bin/re_dms

deploy: build
	cp bin/re_dms roles/re_dms/files/re_dms
	ansible-playbook -i hosts re_dms.yml

clean:
	rm -rf bin
	docker rm -f re_dms