FROM public.ecr.aws/d3j8x8q7/olympus-base-go:latest

WORKDIR /app

COPY . /app

RUN cd /app/backend && go mod download

CMD ["/bin/bash"]
