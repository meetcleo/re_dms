all: build

build: clean
	mkdir -p bin
	docker build --platform linux/amd64 -t re_dms .
	docker create --platform linux/amd64 -ti --name re_dms re_dms bash
	docker cp re_dms:/tmp/re_dms/target/release/re_dms ./bin/re_dms

deploy_staging: build
	cp bin/re_dms roles/re_dms/files/re_dms
	ansible-playbook -i hosts re_dms.yml --tags re_dms --skip-tags copy_config -e "env=staging"

deploy: deploy_staging
	ansible-playbook -i hosts re_dms.yml --tags re_dms --skip-tags copy_config -e "env=production"

deploy_staging_with_config: build
	cp bin/re_dms roles/re_dms/files/re_dms
	ansible-playbook -i hosts re_dms.yml --tags "re_dms,copy_config" -e "env=staging"

deploy_with_config: build
	cp bin/re_dms roles/re_dms/files/re_dms
	ansible-playbook -i hosts re_dms.yml --tags "re_dms,copy_config" -e "env=production"

clean:
	rm -rf bin
	rm -rf roles/re_dms/files/re_dms
	docker rm -f re_dms
