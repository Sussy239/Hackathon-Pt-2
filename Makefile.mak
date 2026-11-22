IMAGE_NAME := shopping-sessions:latest

build:
	docker build --platform linux/amd64 -t $(IMAGE_NAME) .

save: build
	docker save $(IMAGE_NAME) | gzip > submission.tar.gz

run-local: build
	mkdir -p ./output ./scratch
	docker run --rm --platform linux/amd64 \
		--cpus=8 --memory=24g \
		--read-only \
		-v $(PWD)/data:/data:ro \
		-v $(PWD)/output:/output \
		-v $(PWD)/scratch:/scratch \
		$(IMAGE_NAME)