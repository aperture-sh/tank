IMAGE := $(DOCKER_ORGANIZATION)/$(notdir $(TRAVIS_REPO_SLUG)):latest
IMAGE_BRANCH := $(DOCKER_ORGANIZATION)/$(notdir $(TRAVIS_REPO_SLUG)):$(TRAVIS_BRANCH)

image:
	docker build -t $(IMAGE) .

image-branch:
	docker tag $(IMAGE) $(IMAGE_BRANCH)

push-image:
	docker push $(IMAGE)

push-image-branch:
	docker push $(IMAGE_BRANCH)


.PHONY: image push-image image-branch push-image-branch
