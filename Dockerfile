# 1️⃣ Chọn base image Node.js
FROM node:20

# 2️⃣ Thư mục làm việc trong container
WORKDIR /usr/src/app

# 3️⃣ Copy package.json và cài dependencies
COPY package*.json ./
RUN npm install --production

# 4️⃣ Copy toàn bộ source code
COPY . .

# 5️⃣ Expose cổng 8080 ra ngoài container
EXPOSE 8000

# 6️⃣ Start app
CMD ["npm", "start"]
