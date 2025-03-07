# For akshare project
# @version 1.0

FROM chariothy/pydata:latest
LABEL maintainer="chariothy@gmail.com"

WORKDIR /app

COPY ./requirements.txt .

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
  && echo 'Asia/Shanghai' > /etc/timezone \
  && pip install --no-cache-dir -r ./requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/

CMD [ "python" ]