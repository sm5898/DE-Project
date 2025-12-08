# CleanBite — Local development setup

This README explains how to set up and run the CleanBite Flask + PySpark application on macOS. It covers system prerequisites, installing Java and MongoDB via Homebrew, creating a Python virtual environment, installing Python dependencies, and running the app so the map and restaurant list work together.

---

## Quick summary (copy-paste)

Run these commands from your project root (where this README lives):

```bash
# create & activate venv (one-time)
python3 -m venv .venv
source .venv/bin/activate

# install Python deps (pyspark is large)
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt  # if present; otherwise install at least flask pymongo pyspark pandas

# make sure Java 17 is installed and use it to run the app
# install Temurin 17 once via Homebrew:
# brew install --cask temurin17

# run app with Java 17 explicitly set for this command
JAVA_HOME=$(/usr/libexec/java_home -v 17) PATH="$JAVA_HOME/bin:$PATH" ./.venv/bin/python -u display/run.py
```

Open `http://127.0.0.1:5027/restaurants` (or the port printed when Flask starts) in your browser.

---

## Prerequisites (macOS)

- Homebrew (package manager) — install from https://brew.sh if you don't have it.
- OpenJDK 17 (Temurin) — required for Spark (Spark 4.x expects Java 17 classfile support).
- MongoDB Community Server 6.0 (or a running MongoDB accessible at `mongodb://127.0.0.1:27017`).
- Python 3.9+ (macOS system Python is fine; we'll create a venv).

### Install via Homebrew

If you have Homebrew installed, run:

```bash
# install (or upgrade) Temurin 17
brew install --cask temurin17

# install MongoDB community server
brew tap mongodb/brew
brew install mongodb-community@6.0

# start MongoDB as a service
brew services start mongodb-community@6.0

# confirm mongo listening
# nc -vz 127.0.0.1 27017
```

Note: if you use a different MongoDB installation or remote host, ensure the URI in `display/run.py` (Spark config) matches your Mongo instance.

---

## Python virtualenv and dependencies

1. From repo root create and activate the venv (run once):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install --upgrade pip setuptools wheel
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  pip install flask pymongo pyspark pandas
fi
```

Add `pandas` to `requirements.txt` if you want reproducible installs.

---

## Run the application

Always start the app with Java 17 available to the process. The simplest: set `JAVA_HOME` for the command.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) PATH="$JAVA_HOME/bin:$PATH" ./.venv/bin/python -u display/run.py
```

This starts Flask (development server) and a SparkSession that reads the `restaurants` collection from MongoDB. The app prints the Flask address and Spark version in the terminal.

If you prefer to make Java 17 your default for all shells, add this to your `~/.zshrc` (optional):

```bash
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

---

## Data & connectors

- The app uses the Mongo Spark connector jars in `display/jars/`. If your Spark initialization expects jar files there, keep the jars in that folder. `display/run.py` collects all `.jar` files in `display/jars` and passes them into Spark via `spark.jars`.
- To populate MongoDB locally, you can use `backend/database_create.py` (it expects `backend/restaurants.csv`). Run it from the project root after setting any needed Mongo connection variables.

---


## Running inside VS Code

- Use the integrated terminal: run the venv activation and the `JAVA_HOME=...` run command above.
- No `start.sh` script is provided by default. Use the direct commands as shown in the previous sections to activate your environment and run the app.

---

## Common issues & troubleshooting

- ModuleNotFoundError: No module named 'pandas' (or pymongo/pyspark): activate the project `.venv` and `pip install pandas` (or run `pip install -r requirements.txt`).
- UnsupportedClassVersionError / Spark classfile version mismatch: ensure you run the process with Java 17. See the `JAVA_HOME=...` commands above.
- MongoDB connection refused to 127.0.0.1:27017: ensure `brew services start mongodb-community@6.0` ran successfully and check `brew services list`. You can also examine Mongo logs at `/usr/local/var/log/mongodb/` (Homebrew path may vary).
- Port conflicts (Flask reports address in use): stop other processes using the port (use `lsof -i :5027` or change Flask port in `display/run.py` `app.run(...)`).

If the app fails at Spark start, capture the terminal output and paste the last 200 lines here so we can diagnose.

---

## Windows setup (PowerShell)

This section explains how to install Java 17, Spark, MongoDB, Hadoop/winutils, configure system environment variables, create a Python virtual environment, and run the CleanBite Flask + PySpark app on Windows.

These steps work for native Windows (PowerShell). If you prefer a Linux-like environment, WSL2 (Ubuntu) also works — in that case follow the macOS instructions inside WSL.

---

### Prerequisites (Windows)

- Java OpenJDK 17 (Temurin) — required for Spark 3.4/3.5 (Spark needs Java 17 classfile compatibility).
- Apache Spark 3.5.x (pre-built for Hadoop 3.3).
- winutils.exe for Hadoop 3.3 (required for Spark on Windows).
- MongoDB Community Server 6.0 (runs as a Windows service).
- Python 3.9+.

---

### Install Java 17 (Temurin)

Download and install from Adoptium:

https://adoptium.net/temurin/releases/?version=17

Verify:

```powershell
java -version
```

Expected output:
```
openjdk version "17.x.x"
```

---

### Install Apache Spark 3.5.x

1. Download Spark 3.5.1 (pre-built for Hadoop 3.3) from:
   https://spark.apache.org/downloads.html

2. Extract to:
   ```
   C:\spark
   ```

You should have:
```
C:\spark\bin
C:\spark\sbin
C:\spark\jars
```

---

### Install Hadoop winutils (required for Spark on Windows)

1. Download Hadoop 3.3.x Windows binaries from:
   https://github.com/steveloughran/winutils

2. Extract to:
   ```
   C:\hadoop
   ```

You must have:
```
C:\hadoop\bin\winutils.exe
```

---

### Install MongoDB Community Server 6.0

Download MSI from:

https://www.mongodb.com/try/download/community

The installer will configure MongoDB as a Windows service.

Verify that port 27017 is listening:

```powershell
netstat -ano | findstr 27017
```

---

### Configure environment variables (System-wide)

Open:

```
Control Panel → System → Advanced System Settings → Environment Variables
```

Add the following under **System variables**:

**JAVA_HOME**
```
C:\Program Files\Eclipse Adoptium\jdk-17\
```
Add to PATH:
```
%JAVA_HOME%\bin
```

**SPARK_HOME**
```
C:\spark
```
Add to PATH:
```
%SPARK_HOME%\bin
```

**HADOOP_HOME**
```
C:\hadoop
```
Add to PATH:
```
%HADOOP_HOME%\bin
```

**⚠️ Restart PowerShell so the changes apply.**

---

### Python virtualenv and dependencies

From the project root (same folder where this README exists):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install --upgrade pip setuptools wheel
if (Test-Path requirements.txt) {
    pip install -r requirements.txt
} else {
    pip install flask pymongo pyspark pandas
}
```

---

### Run the application

Ensure Java, Spark, and Hadoop variables are available in the current session (PowerShell must be restarted after setting system variables).

Activate venv and run:

```powershell
.\.venv\Scripts\Activate.ps1
python .\display\run.py
```

You should see Spark initialize:

```
INFO SparkContext: Running Spark version 3.5.1
 * Running on http://127.0.0.1:5027
```

Open the browser at the printed Flask URL, typically:

```
http://127.0.0.1:5027/restaurants
```

---

### Data & connectors

Keep all MongoDB connector jars inside:

```
display/jars/
```

Your `run.py` automatically loads all `*.jar` files from this folder into Spark.

**Do not move or rename them.**

---

### Common issues & troubleshooting (Windows)

- **"Could not find winutils.exe"**
  - Ensure `C:\hadoop\bin\winutils.exe` exists and `HADOOP_HOME` is set correctly.

- **UnsupportedClassVersionError**
  - Spark detected Java 8/21 — ensure `java -version` shows Java 17.

- **Mongo connection refused**
  - Confirm MongoDB service is running:
    ```
    services.msc → MongoDB Server should be "Running"
    ```

- **ModuleNotFoundError (pandas/pymongo/pyspark)**
  - Your venv isn't activated; run:
    ```powershell
    .\.venv\Scripts\Activate.ps1
    ```

- **Flask port already in use**
  - Modify `app.run(...)` port in `display/run.py`, or stop the conflicting process.

---

### Optional: Windows start script

You can create a script to avoid resetting environment variables manually.

Create `start-windows.ps1`:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17\"
$env:SPARK_HOME = "C:\spark"
$env:HADOOP_HOME = "C:\hadoop"

$env:Path = "$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"

.\.venv\Scripts\Activate.ps1
python display/run.py
```

Run with:

```powershell
powershell -ExecutionPolicy Bypass .\start-windows.ps1
```

## Optional: VM or container setups

- Multipass: you can create an Ubuntu VM and mount the project directory into it, then run the same steps inside the VM. This is useful if you want an isolated environment. The project previously included a `provision-multipass.sh` helper (you may re-create one or run the manual steps described above inside the VM).
- Docker: you can containerize the app, but Spark and PySpark inside container require care (JVM + connector jars). If you want a Dockerfile + docker-compose setup, I can add one.

--