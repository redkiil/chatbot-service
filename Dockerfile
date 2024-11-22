# Step 1: Use the Node.js base image
FROM node:20

# Step 2: Update apt-get and install required tools
RUN apt-get update && apt-get install -y \
    telnet \
    iputils-ping \
    traceroute \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Step 3: Set the working directory
WORKDIR /app

# Step 4: Copy package.json and package-lock.json for dependency installation
COPY package*.json ./

# Step 5: Install dependencies
RUN npm install

# Step 6: Copy the rest of the application files
COPY . .

# Step 7: Compile TypeScript to JavaScript
RUN npx tsc

# Step 8: Expose the port if necessary (Optional)
# EXPOSE 3000

# Step 9: Start the application
CMD ["node", "dist/index.js"]
