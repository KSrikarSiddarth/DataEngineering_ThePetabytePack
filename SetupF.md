# SetupF: Windows master + Ubuntu worker + MinIO, all in Azure

This is the complete guide for running `DataEngineering_ThePetabytePack` on a two-VM Spark standalone cluster in Azure.
MinIO on the Ubuntu VM holds all the data, so no Azure storage account, no Tailscale, no Docker and no egress are needed.
You need no other file: it covers creating the VMs, SSH, Spark, MinIO, the project, the stream path and troubleshooting.
(`SetupC.md` and `minIOInstall.md` are the earlier, partial versions.)

**Status:**
- **Checked:** the project code was checked with its test suite on the laptop (section 17).
- **Not run yet:** these steps haven't been run on the VMs. Commands marked *(unverified)* are ones I couldn't check at all.

---

## 0. Overview

### 0.1 Who does what

| Machine | Role | Runs |
|---|---|---|
| **VM1** `spark-master` (Windows 11 or Windows Server) | Spark **master** and **driver** | The master (scheduled task) and `scripts\run_on_cluster.ps1`. Holds no data |
| **VM2** `spark-worker` (Ubuntu 24.04, 4 vCPU, 8 GB) | Spark **worker**, **MinIO**, data and analysis | Executors, MinIO (buckets `bronze` and `lake`), downloads, reconcile, queries, dashboard, optional Kafka |
| **Your machine** | Editor and viewer | SSH, `scp`, and SSH tunnels to the web UIs |

```
 Your machine --ssh/tunnels--> Azure VNet (one region, one subnet, private IPs)
                                +-----------------------------+        +-------------------------------+
                                | VM1  Windows                |        | VM2  Ubuntu 24.04             |
                                |  Spark master :7077 (UI 8080)|<------ |  Spark worker (UI 8081)       |
                                |  driver :40000-40002 (UI 4040)|<-----> |  executors                    |
                                |  run_on_cluster.ps1         |        |  MinIO :9000 (console 9001)   |
                                |  .env.setupF (MinIO keys)   | -----> |   bronze = input, lake = output|
                                +-----------------------------+        |  data/, output/, dashboard    |
                                                                       |  optional Kafka :9092         |
                                                                       +-------------------------------+
```

- **Data flow:** executors on VM2 read `s3a://bronze` and write `s3a://lake` over loopback. The driver on VM1 reads only file listings and Parquet footers from MinIO, plus small results.
- **Credentials:** the driver sends the MinIO keys to the executors with the Spark config. VM2's Spark install needs no MinIO settings.

### 0.2 Ports

| Port | On | Used by | Open to |
|---|---|---|---|
| 22 | VM1, VM2 | SSH | Your IP only (NSG) |
| 7077 | VM1 | Worker registers with the master | VNet (Windows Firewall) |
| 8080 | VM1 | Master web UI | VNet (via tunnel) |
| 4040 | VM1 | Job web UI while a job runs | VNet (via tunnel) |
| 18080 | VM1 | History server (optional) | VNet (via tunnel) |
| 40000-40002 | VM1 | Executors talk to the driver | VNet (Windows Firewall) |
| 8081 | VM2 | Worker web UI | VNet (via tunnel) |
| 9000 | VM2 | MinIO S3 API (Spark, `mc`, DuckDB) | VNet only. **Never the internet** |
| 9001 | VM2 | MinIO web console | Localhost only (SSH tunnel) |
| 9092 | VM2 | Kafka (optional) | VNet |

Azure's default NSG rules already allow all traffic **between VMs in the same VNet**. So you only add NSG rules for SSH from your IP. Never add public rules for the other ports.

### 0.3 Placeholders and conventions

| Placeholder | Meaning |
|---|---|
| `<RG>` | Resource group, e.g. `taxi-rg` |
| `<MASTER_IP>` | VM1's **private** IP, e.g. `10.0.0.4` |
| `<VM2_IP>` | VM2's **private** IP, e.g. `10.0.0.5`. It is also the MinIO host |
| `<vm1-public-ip>`, `<vm2-public-ip>` | Public IPs, used only for SSH from your machine |
| `<VNET_RANGE>` | The VNet address range, e.g. `10.0.0.0/16` |
| `<MINIO_PASSWORD>` | The MinIO root password you choose (at least 8 characters) |

Each code block says where it runs:
- **[your machine]**: your laptop.
- **[VM1 PowerShell]**: an SSH session to VM1. Administrator rights are only needed where it says "admin". The Azure admin account is already an administrator.
- **[VM2 bash]**: an SSH session to VM2.

### 0.4 Order of work

1. Create or check the VMs (section 1), then set up SSH (section 2).
2. Install and configure Spark on VM1 and start the master (sections 3 and 4).
3. Install Spark on VM2, start the worker and check the cluster (sections 5 and 6).
4. Install MinIO on VM2 (section 7).
5. Set up the project on both VMs (section 8).
6. Land the data, run the jobs and read the results (sections 9 to 11).
7. Optionally, add the Kafka stream (section 12).

---

## 1. Azure resources

### 1.1 Resource budget

| | VM1 (Windows) | VM2 (Ubuntu) |
|---|---|---|
| Size | 2 to 4 vCPU, 8 GB (e.g. `Standard_D2s_v5` / `D4s_v5`) | 4 vCPU, 8 GB (e.g. `Standard_D4als_v6`) |
| Memory use | Windows ~2 to 3 GB, driver 2 GB | Ubuntu ~0.7 GB, MinIO ~0.5 to 1 GB, worker 4 GB, Kafka 1 GB (optional) |
| Disk | C: holds Windows. Spark temp files are small on the driver | OS disk (~27 GB free): MinIO data, `data/`, `output/`, Spark spill. Add a data disk for more (section 7.9) |

Windows itself uses a lot of RAM, so VM1 runs no worker.

### 1.2 Create the VMs (skip if they exist)

**[your machine]** with the Azure CLI (`az login` first):

```bash
RG=taxi-rg; LOC=centralindia
az group create -n $RG -l $LOC

# VM2: Ubuntu worker (creates the VNet and subnet)
az vm create -g $RG -n spark-worker --image Canonical:ubuntu-24_04-lts:server:latest \
  --size Standard_D4als_v6 --admin-username azureuser --generate-ssh-keys --public-ip-sku Standard

az network vnet list -g $RG -o table                  # note the VNet and subnet names
az network vnet subnet list -g $RG --vnet-name <vnet-name> -o table

# VM1: Windows, in the SAME VNet and subnet. Windows Server:
az vm create -g $RG -n spark-master \
  --image MicrosoftWindowsServer:WindowsServer:2022-datacenter-azure-edition:latest \
  --size Standard_D4s_v5 --admin-username azureuser --admin-password '<strong-password>' \
  --public-ip-sku Standard --vnet-name <vnet-name> --subnet <subnet-name>
```

**Windows 11 instead of Server** *(unverified)*:
- Use a Windows 11 image from the `MicrosoftWindowsDesktop` publisher and add `--license-type Windows_Client`.
- That license type needs eligible licensing, such as a Visual Studio subscription.
- List the image names with `az vm image list --publisher MicrosoftWindowsDesktop --offer windows-11 --all -o table`.

### 1.3 Private IPs (write these down)

**[your machine]**:

```bash
az vm show -d -g $RG -n spark-master --query privateIps -o tsv     # <MASTER_IP>
az vm show -d -g $RG -n spark-worker --query privateIps -o tsv     # <VM2_IP>
az vm show -d -g $RG -n spark-master --query publicIps -o tsv      # <vm1-public-ip>
az vm show -d -g $RG -n spark-worker --query publicIps -o tsv      # <vm2-public-ip>
az network vnet show -g $RG -n <vnet-name> --query addressSpace.addressPrefixes -o tsv   # <VNET_RANGE>
```

Both private IPs must be in the same VNet. Every Spark and MinIO setting below uses the **private** IPs.

**Keep the private IPs fixed:**
- In the portal, open each VM, then **Networking → the NIC → IP configurations → ipconfig1**, and set **Static**.
- This keeps the IPs from changing after a deallocate, so the configs stay valid.
- The public IP may still change after a deallocate (unless it is static). Update `~/.ssh/config` (section 2.3) when it does.

### 1.4 NSG: SSH from your IP only

**[your machine]**:

```bash
curl -s ifconfig.me                                       # your public IP
az vm open-port -g $RG -n spark-worker --port 22 --priority 900
az vm open-port -g $RG -n spark-master --port 22 --priority 900
```

Then, in the portal, open each VM's **Networking**, edit the port 22 rule, and set **Source** to **IP Addresses** with your IP.

For Windows, `az vm create` also opens **RDP (3389)** to the world. Restrict it to your IP, or delete it once SSH works.

---

## 2. SSH access

### 2.1 VM2 (Ubuntu)

**[your machine]**:

```bash
chmod 400 ~/.ssh/<key>.pem                    # "permissions too open" otherwise
ssh -i ~/.ssh/<key>.pem azureuser@<vm2-public-ip>
```

On Windows, use `icacls <key>.pem /inheritance:r /grant:r "%USERNAME%:R"` instead of `chmod`.

### 2.2 VM1 (Windows): turn on the OpenSSH server

Azure Windows VMs don't run SSH by default. Turn it on without RDP:

**[your machine]**:

```bash
az vm run-command invoke -g $RG -n spark-master --command-id RunPowerShellScript --scripts \
  "Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0; \
   Set-Service sshd -StartupType Automatic; Start-Service sshd; \
   New-ItemProperty -Path HKLM:\SOFTWARE\OpenSSH -Name DefaultShell -Value C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -PropertyType String -Force; \
   if (-not (Get-NetFirewallRule -Name sshd -ErrorAction SilentlyContinue)) { New-NetFirewallRule -Name sshd -DisplayName 'OpenSSH Server' -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 }"
```

- **Timing:** this takes a few minutes.
- **Alternative:** RDP in once and run the same lines in an administrator PowerShell. Afterwards, **sign out** rather than just closing the window, because an open desktop costs RAM.
- **Connect:** `ssh azureuser@<vm1-public-ip>` with the admin password. You land in PowerShell, because of the `DefaultShell` line above.

**Key login instead of a password.** Windows reads keys for administrator accounts from a special file.
1. **[your machine]** If you have no key, create one with `ssh-keygen -t ed25519`. Then copy it over:
   ```bash
   scp ~/.ssh/id_ed25519.pub azureuser@<vm1-public-ip>:C:/ProgramData/ssh/administrators_authorized_keys
   ```
   To reuse the Azure `.pem`, its public key is `ssh-keygen -y -f ~/.ssh/<key>.pem`.
2. **[VM1 PowerShell, admin]** Set the file's permissions. sshd ignores the file otherwise:
   ```powershell
   icacls C:\ProgramData\ssh\administrators_authorized_keys /inheritance:r /grant "Administrators:F" /grant "SYSTEM:F"
   ```

### 2.3 `~/.ssh/config` on your machine

```
Host spark-master
    HostName <vm1-public-ip>
    User azureuser
    IdentityFile ~/.ssh/id_ed25519
Host spark-worker
    HostName <vm2-public-ip>
    User azureuser
    IdentityFile ~/.ssh/<key>.pem
```

From now on, use `ssh spark-master`, `ssh spark-worker`, `scp ... spark-worker:...`, and so on.

**Several sessions:** SSH supports several sessions to the same VM at once. Use one session for running jobs and another for watching them.

---

## 3. VM1: install Java, Spark, Python and winutils

**[VM1 PowerShell, admin]**. The versions must match VM2: **Spark 3.5.3, Python 3.12.x, Java 17**.

```powershell
New-Item -ItemType Directory -Force C:\java, C:\spark-tmp, C:\spark-events, C:\spark-logs, C:\hadoop\bin | Out-Null
cd $env:TEMP

# Java 17 (Temurin zip)
Invoke-WebRequest "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse" -OutFile jdk17.zip
Expand-Archive jdk17.zip C:\java -Force
$jdk = (Get-ChildItem C:\java -Directory | Select-Object -First 1).FullName

# Spark 3.5.3 (tar.exe ships with Windows 10/11 and Server 2019+)
Invoke-WebRequest "https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz" -OutFile spark.tgz
tar -xzf spark.tgz -C C:\
Rename-Item C:\spark-3.5.3-bin-hadoop3 C:\spark

# Python 3.12, silent, for all users (any 3.12.x matches Ubuntu 24.04's 3.12)
Invoke-WebRequest "https://www.python.org/ftp/python/3.12.8/python-3.12.8-amd64.exe" -OutFile python-installer.exe
.\python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 TargetDir=C:\Python312 | Out-Null

# Machine-wide environment variables (visible to scheduled tasks). [Environment] avoids setx's 1024-character PATH limit.
[Environment]::SetEnvironmentVariable("JAVA_HOME", $jdk, "Machine")
[Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark", "Machine")
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")
$path = [Environment]::GetEnvironmentVariable("PATH", "Machine")
[Environment]::SetEnvironmentVariable("PATH", "$path;$jdk\bin;C:\spark\bin;C:\hadoop\bin", "Machine")
```

**winutils:** this is Hadoop's Windows helper.
- Put `winutils.exe` **and** `hadoop.dll` built for Hadoop **3.3.x** into `C:\hadoop\bin`.
- They are only available from third-party repositories, such as the `hadoop-3.3.x/bin` folder of the `cdarlint/winutils` GitHub repository.
- Download them yourself and verify the source. Copy them over with `scp winutils.exe hadoop.dll spark-master:C:/hadoop/bin/`.

**Check.** Close the SSH session and reconnect so the new variables load, then:

```powershell
java -version                         # 17.x
python --version                      # 3.12.x
spark-submit --version                # 3.5.3
dir C:\spark\jars\hadoop-client-api*  # hadoop-client-api-3.3.4.jar: Spark's bundled Hadoop is 3.3.4
C:\hadoop\bin\winutils.exe ls C:\     # lists C:\ without an error
```

`hadoop-aws` (section 10) must match the bundled Hadoop version, **3.3.4**.

**PowerShell scripts.** Windows 11 blocks `.ps1` scripts by default. Allow local scripts for your user:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned -Force
```

---

## 4. VM1: firewall, Spark config and the master

### 4.1 Windows Firewall

**[VM1 PowerShell, admin]**. Open the Spark ports to the VNet only:

```powershell
New-NetFirewallRule -DisplayName "Spark" -Direction Inbound -Protocol TCP `
  -LocalPort 7077,8080,4040,18080,40000-40002 -RemoteAddress <VNET_RANGE> -Action Allow
```

### 4.2 Spark config files

**[VM1 PowerShell, admin]**. Replace `<MASTER_IP>` first. Forward slashes work in Windows paths here.

```powershell
Set-Content C:\spark\conf\spark-env.cmd "set SPARK_LOCAL_DIRS=C:\spark-tmp"

Set-Content C:\spark\conf\spark-defaults.conf @"
spark.sql.adaptive.enabled          true
spark.driver.host                   <MASTER_IP>
spark.driver.port                   40000
spark.blockManager.port             40001
spark.driver.blockManager.port      40002
spark.local.dir                     C:/spark-tmp
spark.eventLog.enabled              true
spark.eventLog.dir                  file:///C:/spark-events
spark.history.fs.logDirectory       file:///C:/spark-events
spark.pyspark.driver.python         C:/Python312/python.exe
spark.pyspark.python                python3
"@
```

- **Fixed driver ports:** the driver uses fixed ports (40000-40002), so the firewall rule above covers them. `spark.driver.host` makes executors call back on the private IP.
- **Mixed OS:** the driver runs the Windows `python.exe`, while executors on Ubuntu run `python3`. `run_on_cluster.ps1` also sets `PYSPARK_PYTHON=python3`.
- **No MinIO keys here:** they come from `.env.setupF` (section 8).
- **Driver memory:** set in `.env.setupF`, 2g by default.

### 4.3 Start the master as a scheduled task

A scheduled task keeps the master running after you log off and restarts it at boot.

**[VM1 PowerShell, admin]**:

```powershell
$cmd = '/c C:\spark\bin\spark-class.cmd org.apache.spark.deploy.master.Master --host <MASTER_IP> --port 7077 --webui-port 8080 > C:\spark-logs\master.log 2>&1'
$action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $cmd
$trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName SparkMaster -Action $action -Trigger $trigger -User SYSTEM -RunLevel Highest
Start-ScheduledTask -TaskName SparkMaster
Start-Sleep 15; Get-Content C:\spark-logs\master.log -Tail 20     # look for "Started MasterWebUI" and "spark://<MASTER_IP>:7077"
```

- **Stop or start:** `Stop-ScheduledTask -TaskName SparkMaster` / `Start-ScheduledTask -TaskName SparkMaster`.
- **History server (optional):** it keeps job details after a job ends. It uses the same pattern with `org.apache.spark.deploy.history.HistoryServer`, task name `SparkHistory`, and log `C:\spark-logs\history.log`. Its UI is on port 18080.

---

## 5. VM2: install Spark and start the worker

### 5.1 Install

**[VM2 bash]**:

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk python3-pip tmux htop
cd ~ && wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar xzf spark-3.5.3-bin-hadoop3.tgz && mv spark-3.5.3-bin-hadoop3 spark && rm spark-3.5.3-bin-hadoop3.tgz
cat >> ~/.bashrc <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOF
source ~/.bashrc
java -version; python3 --version; spark-submit --version        # 17.x, 3.12.x, 3.5.3
ls $SPARK_HOME/jars | grep hadoop-client                         # 3.3.4, same as VM1
```

The project's Python packages (pyspark, numpy, setuptools, and so on) are installed in section 8.3.

### 5.2 Worker settings

The worker gets 4 GB and 3 cores. The rest of VM2's 8 GB is for Ubuntu, MinIO and, optionally, Kafka.

**[VM2 bash]**:

```bash
cat > $SPARK_HOME/conf/spark-env.sh <<'EOF'
export SPARK_WORKER_MEMORY=4g
export SPARK_WORKER_CORES=3
export SPARK_LOCAL_DIRS=/mnt/spark-tmp
EOF
sudo mkdir -p /mnt/spark-tmp && sudo chown $USER /mnt/spark-tmp
```

- **`>` vs `>>`:** `>` writes the file fresh. Use `>>` only to add lines to an existing file.
- **Spill location:** on a VM size without a temp disk, such as `D4als_v6`, `/mnt/spark-tmp` is on the OS disk, shared with MinIO.

### 5.3 Start the worker as a service

A service survives logouts and starts at boot. Replace `<MASTER_IP>`.

**[VM2 bash]**:

```bash
sudo tee /etc/systemd/system/spark-worker.service >/dev/null <<EOF
[Unit]
Description=Spark worker
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
Environment=SPARK_HOME=$HOME/spark
ExecStart=$HOME/spark/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://<MASTER_IP>:7077
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now spark-worker
journalctl -u spark-worker -n 20 --no-pager        # look for "Successfully registered with master spark://<MASTER_IP>:7077"
```

- **The heredoc:** it uses `<<EOF` without quotes, so `$USER` and `$HOME` are filled in. Check the result with `cat /etc/systemd/system/spark-worker.service`.
- **Worker settings:** `spark-class` reads `spark-env.sh`, so the 4g and 3-core settings apply.
- **Without the service:** `start-worker.sh spark://<MASTER_IP>:7077` works too. You'd have to run it again after every reboot, and stop it with `stop-worker.sh`.

**After changing `spark-env.sh`:** run `sudo systemctl restart spark-worker`.

---

## 6. Check the cluster

**[your machine]**: open tunnels and look at the UIs.

```bash
ssh -N -L 8080:<MASTER_IP>:8080 -L 4040:<MASTER_IP>:4040 -L 18080:<MASTER_IP>:18080 spark-master &
ssh -N -L 8081:localhost:8081 spark-worker &
```

- **`http://localhost:8080` (master):** one worker, **ALIVE**, with 3 cores and 4.0 GiB.
- **`http://localhost:8081`:** the worker's own UI.
- **`:4040`:** only exists while a job runs.
- **`:18080`:** the history server, if started.

| Check | Where | Expected |
|---|---|---|
| Same versions | `spark-submit --version`, `python --version` on both | 3.5.3 and 3.12.x |
| VM2 can reach the master | **[VM2 bash]** `nc -zv <MASTER_IP> 7077` | `succeeded` |
| Worker registered | Master UI | VM2 ALIVE |

---

## 7. MinIO on VM2

MinIO is a self-hosted object store with an S3-compatible API. Spark reads and writes it through `s3a://`.
"S3" is only the name of the protocol: no AWS account is involved.

MinIO keeps everything as plain files in one folder, here `/data/minio`.

> **Security:** MinIO Community Edition is archived (source-only, known CVEs). That's fine for a private demo inside the
> VNet. Never open port 9000 or 9001 to the internet, and use a strong password.

### 7.1 Get the binary

**[VM2 bash]**. First try the last published binary *(unverified: it may no longer be served)*:

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio && sudo mv minio /usr/local/bin/
minio --version
```

If the download fails, build from source *(unverified)*. Ubuntu's `apt` Go is too old, so get Go from go.dev:

```bash
wget https://go.dev/dl/go1.24.4.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.4.linux-amd64.tar.gz && rm go1.24.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go install github.com/minio/minio@latest
sudo mv ~/go/bin/minio /usr/local/bin/
rm -rf ~/go/pkg                          # frees the 1 to 2 GB build cache
minio --version
```

If Go reports a version error, install the Go version it names.

### 7.2 User, data folder and config

**[VM2 bash]**. Choose `<MINIO_PASSWORD>` with letters and digits only. That avoids the quoting problems behind
the most common error (section 7.6).

```bash
sudo useradd -r -s /sbin/nologin minio-user
sudo mkdir -p /data/minio && sudo chown minio-user:minio-user /data/minio

sudo tee /etc/default/minio >/dev/null <<'EOF'
MINIO_VOLUMES="/data/minio"
MINIO_OPTS="--address :9000 --console-address :9001"
MINIO_ROOT_USER=sparkadmin
MINIO_ROOT_PASSWORD=<MINIO_PASSWORD>
EOF
sudo chmod 600 /etc/default/minio
sudo cat -A /etc/default/minio            # every line must end in "$" only: "^M$" means Windows line endings
```

### 7.3 Run MinIO as a service

**[VM2 bash]**:

```bash
sudo tee /etc/systemd/system/minio.service >/dev/null <<'EOF'
[Unit]
Description=MinIO
After=network-online.target
Wants=network-online.target

[Service]
User=minio-user
Group=minio-user
EnvironmentFile=/etc/default/minio
ExecStart=/usr/local/bin/minio server $MINIO_OPTS $MINIO_VOLUMES
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now minio
systemctl status minio --no-pager
curl -I http://localhost:9000/minio/health/live        # HTTP/1.1 200 OK
```

MinIO reads `/etc/default/minio` only at start-up. After editing the file, run `sudo systemctl restart minio`.

### 7.4 Client (`mc`) and buckets

**[VM2 bash]**:

```bash
wget https://dl.min.io/client/mc/release/linux-amd64/mc          # (unverified) or: go install github.com/minio/mc@latest
chmod +x mc && sudo mv mc /usr/local/bin/
mc alias set local http://localhost:9000 sparkadmin '<MINIO_PASSWORD>'
mc mb local/bronze local/lake
mc ls local                                                      # bronze/ and lake/
```

- **Port:** use port **9000**, not 9001, which is the web console.
- **Quoting:** put the password in **single quotes**. Double quotes let bash change `$`, `!` and backticks.
- **The alias name:** the project scripts use the alias `local`.

### 7.5 Check access from VM1

**[VM1 PowerShell]**:

```powershell
Invoke-WebRequest http://<VM2_IP>:9000/minio/health/live -UseBasicParsing | Select-Object StatusCode    # 200
```

If this times out:
1. Check that MinIO is running: `systemctl status minio`.
2. Check that you used the private IP.
3. Check that both VMs are in the same VNet.
4. Check `ufw` on VM2. It is inactive by default (`sudo ufw status`). If it is active, run `sudo ufw allow from <VNET_RANGE> to any port 9000`.

**The web console:** open `ssh -N -L 9001:localhost:9001 spark-worker` **[your machine]**, browse to `http://localhost:9001`,
and log in with `sparkadmin` and the password.

### 7.6 Error: "The request signature we calculated does not match"

This means the user or password you gave doesn't match what the server is running with.

| Cause | Fix |
|---|---|
| `/etc/default/minio` edited after MinIO started | `sudo systemctl restart minio` |
| Password in double quotes in `mc alias set` | Re-run it with **single** quotes |
| Hidden `\r` (`^M$` in `sudo cat -A /etc/default/minio`) | `sudo sed -i 's/\r$//' /etc/default/minio`, then restart |
| Trailing spaces, or a different user name | Fix the file, restart, re-create the alias |
| Wrong port (9001) | Use 9000 |
| Password changed after the first start | `sudo systemctl stop minio && sudo rm -rf /data/minio/.minio.sys && sudo systemctl start minio`, then `mc mb` the buckets again. This resets MinIO's internal settings; uploaded files stay |
| Clock wrong (usually gives "RequestTimeTooSkewed") | `timedatectl` must show "System clock synchronized: yes" |

Use the same password, **without quotes**, as `S3_SECRET_KEY` in `.env.setupF` on VM1.

### 7.7 Space

| Item | Size |
|---|---|
| MinIO binary and config | ~150 MB |
| Go build cache (only if built from source, deletable) | 1 to 2 GB |
| Data | ~1× what you store, plus a few % metadata (no replication on one drive) |

For this project, total storage is about 2 to 2.5 × the Bronze size:
- One month of Yellow, Green and FHV: about 0.2 GB.
- Three months of all four datasets: about 4 GB.

### 7.8 Temp disks

MinIO can use a temp disk, but don't put MinIO there:
- It is **wiped** when the VM is deallocated, resized or moved.
- On Windows it also holds the pagefile.

A temp disk is fine for Spark spill.

### 7.9 Later: move MinIO to an attached data disk

The path stays `/data/minio`, so no config changes.

1. **Attach a disk.** In the portal: `spark-worker` → **Disks → Create and attach a new disk**. A 64 GB Standard SSD costs a few dollars a month and persists.
2. **[VM2 bash] Format it and copy the data:**
   ```bash
   sudo systemctl stop minio
   lsblk                                              # the new disk, e.g. /dev/sdc, has no partitions
   sudo mkfs.ext4 /dev/sdc
   sudo mkdir -p /mnt/newdisk && sudo mount /dev/sdc /mnt/newdisk
   sudo rsync -aH /data/minio/ /mnt/newdisk/minio/    # keeps ownership and the hidden .minio.sys folder
   sudo umount /mnt/newdisk
   ```
3. **Mount it at `/data`:**
   ```bash
   sudo mv /data /data.old && sudo mkdir /data
   echo "UUID=$(sudo blkid -s UUID -o value /dev/sdc) /data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
   sudo mount -a && df -h /data
   ```
4. **Restart MinIO and check:**
   ```bash
   sudo systemctl start minio && mc ls local/bronze
   ```
5. **Clean up:** once everything looks right, run `sudo rm -rf /data.old`.

**Shortcut:** skip the copy, mount the empty disk, recreate `/data/minio` (owned by `minio-user`) and the buckets, and land the data again.

---

## 8. Project setup

### 8.1 Copy the code (no data)

**[your machine]**:

```bash
cd ~/siddu/MTech/DataEngineeringAtScale
tar czf /tmp/petabyte.tgz --exclude=data --exclude=output --exclude=.git --exclude=__pycache__ \
    --exclude='.env.*' --exclude=.venv DataEngineering_ThePetabytePack
scp /tmp/petabyte.tgz spark-master:C:/petabyte.tgz
scp /tmp/petabyte.tgz spark-worker:~/
```

- **[VM1 PowerShell]** `tar -xzf C:\petabyte.tgz -C C:\` gives `C:\DataEngineering_ThePetabytePack`.
- **[VM2 bash]** `tar -xzf ~/petabyte.tgz -C ~` gives `~/DataEngineering_ThePetabytePack`.

Repeat this after every code change, since both VMs need the same code.

**Config:** `config.yaml` travels with the code. VM1's copy decides the job settings, such as shuffle partitions and the batch month range.

### 8.2 VM1: Python packages and `.env.setupF`

**[VM1 PowerShell]**:

```powershell
cd C:\DataEngineering_ThePetabytePack
python -m pip install -r spark\requirements.txt           # pyspark 3.5.3, PyYAML, numpy, setuptools
```

Write `.env.setupF`. Notepad doesn't work over SSH, so use a here-string, and fill in the values:

```powershell
Set-Content .env.setupF @"
S3_ENDPOINT=http://<VM2_IP>:9000
S3_ACCESS_KEY=sparkadmin
S3_SECRET_KEY=<MINIO_PASSWORD>
MASTER_IP=<MASTER_IP>
DRIVER_MEMORY=2g
EXECUTOR_MEMORY=3g
EXECUTOR_CORES=3
TOTAL_CORES=3
KAFKA_BOOTSTRAP=
"@
Get-Content .env.setupF
```

- **Use private IPs**, never `localhost`. The executors on VM2 receive the same settings.
- **Values unquoted:** the script reads `KEY=VALUE` lines as they are.
- **Keep it private:** the file holds the MinIO password. Git ignores it (`.gitignore`).

### 8.3 VM2: Python packages

**[VM2 bash]**:

```bash
cd ~/DataEngineering_ThePetabytePack
pip install --break-system-packages -r requirements-dev.txt     # executors need numpy + setuptools; also download, analysis, dashboard
chmod +x scripts/*.sh
mc ls local                                                     # bronze/ and lake/
```

- **Where the packages go:** without `sudo`, pip installs them for your user. The worker service runs as your user, so its executors see them.
- **Kafka:** the optional stream also needs `producer/requirements.txt` (section 12).

---

## 9. Land the data (VM2)

**[VM2 bash]**. Start with one month of the three small datasets (about 83 MB):

```bash
cd ~/DataEngineering_ThePetabytePack
scripts/land_data_minio.sh --months 2025-12 --datasets yellow green fhv
mc ls --recursive local/bronze
```

What it does:
- Runs `scripts/download_data.py --zones`, which downloads into `data/`, plus `data/taxi_zones/taxi_zone_lookup.csv` and `data/tlc_published.json`.
- Runs `mc mirror` from `data/` to `local/bronze`.

`data/` stays on VM2, because `reconcile.py --batch` compares the source files with Silver.

You can also run it from your machine: `ssh spark-worker 'cd ~/DataEngineering_ThePetabytePack && scripts/land_data_minio.sh --months 2025-12 --datasets yellow green fhv'`.

**Widen step by step:**

```bash
scripts/land_data_minio.sh --months 2025-11 --datasets yellow green fhv   # a second month (ML needs two or more)
scripts/land_data_minio.sh --months 2025-12 --datasets hvfhv              # about 482 MB; Silver about 1.5 to 2 GB
scripts/land_data_minio.sh --batch                                        # config.yaml range 2025-10 to 2025-12, about 1.7 GB
```

**Disk budget (VM2, ~27 GB free on the OS disk):** the same disk holds all of these:
- each input file twice, in `data/` and in MinIO;
- Silver in `lake`;
- pulled copies in `output/`;
- Spark spill.

Keep the total under about 20 GB. Check with `df -h /` before adding HVFHV. If space runs out, delete months you have
already reconciled from `data/` and `output/`, or attach a data disk (section 7.9).

---

## 10. Run the jobs (VM1)

### 10.1 Batch job

**[VM1 PowerShell]**:

```powershell
cd C:\DataEngineering_ThePetabytePack
scripts\run_on_cluster.ps1 batch --months 2025-12 --datasets yellow green fhv
```

`run_on_cluster.ps1` does the following:
1. Reads `.env.setupF` and sets:
   - the master URL `spark://<MASTER_IP>:7077`;
   - the driver and executor memory and cores;
   - `PYSPARK_PYTHON=python3`;
   - the MinIO endpoint and keys;
   - the packages `hadoop-aws:3.3.4` and `aws-java-sdk-bundle:1.12.262`.
2. Runs `spark/batch_job.py --input-dir s3a://bronze --output-dir s3a://lake/batch`.

Other options, such as `--months` and `--datasets`, pass through. Without `--months`, the job runs the whole batch range in `config.yaml`.

- **First run:** Spark downloads the two packages (about 300 MB) into `%USERPROFILE%\.ivy2`, so VM1 needs internet access. Later runs reuse them.
- **Output:** `lake/batch/silver/...`, `lake/batch/silver_invalid/...` and `lake/batch/gold/...`.
- **Per file:** the job prints a status, row counts, seconds and rows/s. A failed file is logged and the run continues, with exit code 1.
- **Reruns:** rerunning a month replaces only that month's partitions.
- **Speed on MinIO:** writes end with a copy instead of a rename (S3 has no rename), so committing is slower than on a local disk. That's fine at demo sizes.

**Watch it:**
- `http://localhost:8080`: the master, with the running application.
- `http://localhost:4040`: jobs, stages, the Executors tab (executors on `<VM2_IP>`, the driver on `<MASTER_IP>`) and shuffle read/write.
- **[VM2 bash]** `htop`, `free -h`, `df -h /`.

### 10.2 ML job

The ML job needs at least two months of Silver.

```powershell
scripts\run_on_cluster.ps1 ml --models fare anomaly      # a subset first (anomaly needs fare)
scripts\run_on_cluster.ps1 ml                            # all ten models
```

It writes `lake/batch/silver/ml_features` and `lake/batch/gold/ml_*`. Settings are in `config.yaml` (`ml:`).
On these nodes, two small months take about 20 to 30 minutes.

### 10.3 Long runs: survive an SSH disconnect

Closing the SSH session kills a job started in it. For long runs, start the job as a scheduled task and follow its log.

**[VM1 PowerShell, admin]**:

```powershell
$run = 'cd C:\DataEngineering_ThePetabytePack; scripts\run_on_cluster.ps1 batch *> C:\spark-logs\batch.log'
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -Command `"$run`""
Register-ScheduledTask -TaskName TaxiBatch -Action $action -User SYSTEM -RunLevel Highest -Force
Start-ScheduledTask -TaskName TaxiBatch
Get-Content C:\spark-logs\batch.log -Wait          # Ctrl+C stops watching, not the job
```

- **Package cache:** the task runs as SYSTEM, so its first run downloads the packages again into SYSTEM's profile.
- **Changing the job:** edit `$run` and register again with `-Force`.

### 10.4 Tuning knobs

| Setting | Where | Default |
|---|---|---|
| Executor memory, cores, total cores | `.env.setupF` | 3g, 3, 3 (fits the 4g, 3-core worker) |
| Driver memory | `.env.setupF` | 2g |
| Shuffle partitions | `config.yaml` → `batch.shuffle_partitions` / `ml.shuffle_partitions` (VM1's copy) | 16 |
| Months and datasets | `--months`, `--datasets`, or `config.yaml` → `batch:` | 2025-10 to 2025-12, all four |

`run_on_cluster.ps1` defines no flag for shuffle partitions. To compare runs (`DEMO.md`), edit `config.yaml` on VM1 between runs and record
`runtime_sec` and `rows_per_sec` from `gold/batch_run_metrics`.

---

## 11. Read the results

### 11.1 On VM2: the project's checks, queries and dashboard

**[VM2 bash]**:

```bash
cd ~/DataEngineering_ThePetabytePack

# 1. Reconcile: every source row is in Silver, invalid, or counted as a duplicate
scripts/pull_output_minio.sh batch/silver batch/silver_invalid
python3 scripts/reconcile.py --batch                       # OK, diff 0 per file

# 2. Queries and the dashboard (Gold, dimensions and the run log are small)
scripts/pull_output_minio.sh batch/gold batch/silver/batch_run_log batch/silver/dim_zone \
    batch/silver/dim_payment_type batch/silver/dim_ratecode batch/silver/dim_hvfhs_company
python3 scripts/combined_views.py                          # writes output/combined.duckdb
python3 scripts/benchmark_queries.py --no-spark            # every KPI query on DuckDB
streamlit run dashboard/app.py --server.address 127.0.0.1
```

- **Arguments:** each argument to `pull_output_minio.sh` is a folder under `lake/`. With none, it copies everything.
- **Silver is large:** pull Silver trips only when you need them, for example for reconcile.
- **New runs:** pull again after each run.
- **The dashboard:** open `ssh -N -L 8501:localhost:8501 spark-worker` **[your machine]**, then `http://localhost:8501`.
- **Ratios:** Gold holds sums and counts, so compute ratios in the query, for example `sum(sum_total_amount) / sum(trips)`.
- **Your own query:** `duckdb output/combined.duckdb`, or a file from `scripts/queries/*.sql`.

### 11.2 On VM1: query Gold straight from MinIO with DuckDB

This copies nothing and fits the small Gold tables.

**[VM1 PowerShell]**:

```powershell
python -m pip install duckdb==1.5.6
python
```

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("""CREATE SECRET (TYPE S3, KEY_ID 'sparkadmin', SECRET '<MINIO_PASSWORD>',
           ENDPOINT '<VM2_IP>:9000', URL_STYLE 'path', USE_SSL false)""")
con.sql("SELECT * FROM read_parquet('s3://lake/batch/gold/<mart>/**/*.parquet', hive_partitioning=true) LIMIT 20").show()
```

- **`<mart>`:** a folder name from `mc ls local/lake/batch/gold/` **[VM2 bash]**.
- **Internet:** `INSTALL httpfs` downloads the extension once.

### 11.3 Other ways

- **Copy files to VM1 with `mc.exe`** *(unverified link)*:
  ```powershell
  Invoke-WebRequest https://dl.min.io/client/mc/release/windows-amd64/mc.exe -OutFile C:\mc.exe
  C:\mc.exe alias set minio http://<VM2_IP>:9000 sparkadmin '<MINIO_PASSWORD>'
  C:\mc.exe mirror minio/lake/batch/gold C:\results\gold
  ```
- **Copy to your machine:** pull on VM2 (section 11.1), then `scp -r spark-worker:~/DataEngineering_ThePetabytePack/output/batch/gold .` **[your machine]**.
- **Browse:** use the MinIO console (section 7.5).
- **Terminal output:** `show()` and the job's progress print in the PowerShell session, or the log, on VM1.

---

## 12. The stream path (optional): native Kafka on VM2

This path has not been run. Memory on VM2 is tight with Kafka (1 GB heap), MinIO and a 3g executor on 8 GB.
Use small `MAX_RECORDS` values, and set `EXECUTOR_MEMORY=2g` in `.env.setupF` if VM2 runs out of memory.

### 12.1 Install and start Kafka (single broker, KRaft)

**[VM2 bash]**. Replace `<VM2_IP>`:

```bash
cd ~ && wget https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz
tar xzf kafka_2.13-3.9.0.tgz && mv kafka_2.13-3.9.0 kafka && rm kafka_2.13-3.9.0.tgz && cd kafka
cat >> config/kraft/server.properties <<EOF
log.dirs=$HOME/kafka-data
auto.create.topics.enable=false
advertised.listeners=PLAINTEXT://<VM2_IP>:9092,CONTROLLER://localhost:9093
EOF
bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/kraft/server.properties
KAFKA_HEAP_OPTS="-Xms512m -Xmx1g" bin/kafka-server-start.sh -daemon config/kraft/server.properties
sleep 10; tail -5 logs/server.log                  # "Kafka Server started"
```

- **Duplicate keys:** the appended lines override the earlier values in the file, because the last value wins.
- **The advertised address:** it must be `<VM2_IP>`, so that the executors and the producers reach the broker.
- **Stopping:** `~/kafka/bin/kafka-server-stop.sh`. Kafka doesn't restart by itself after a reboot, so run the start line again.

### 12.2 Topics and producers

**[VM2 bash]**:

```bash
cd ~/DataEngineering_ThePetabytePack
pip install --break-system-packages -r producer/requirements.txt          # confluent-kafka
scripts/land_data_minio.sh --months 2026-01 --datasets yellow green fhv    # the streamed month (producers read data/)
export KAFKA_BOOTSTRAP=<VM2_IP>:9092
python3 scripts/create_topics.py                                           # "ok 5 topics ready"

for d in yellow green fhv; do
  MAX_RECORDS=20000 REPLAY_SPEEDUP=0 QUALITY_PROFILE=clean STATS_DIR=output/producer-stats \
    python3 producer/producer.py --dataset $d &
done; wait
```

- **A realistic stream:** drop `REPLAY_SPEEDUP=0` and `QUALITY_PROFILE=clean`. The defaults in `config.yaml` replay one simulated hour per second and inject faults.
- **Summaries:** each producer writes one to `output/producer-stats/<dataset>.json`.
- **Look inside Kafka:**
  ```bash
  ~/kafka/bin/kafka-get-offsets.sh --bootstrap-server <VM2_IP>:9092 --topic taxi.green.raw
  ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server <VM2_IP>:9092 --topic taxi.yellow.raw \
    --from-beginning --max-messages 5 --property print.key=true --property print.headers=true
  ```

### 12.3 Stream job on the cluster

**[VM1 PowerShell]**. First set `KAFKA_BOOTSTRAP=<VM2_IP>:9092` in `.env.setupF`, then:

```powershell
scripts\run_on_cluster.ps1 stream --trigger-mode available_now     # process what is in Kafka, then stop
```

- **Packages:** the script adds the Kafka connector package (`spark-sql-kafka-0-10_2.12:3.5.3`).
- **Output:** Silver, Gold and checkpoints go to `s3a://lake/stream` and `s3a://lake/checkpoints`.
- **Live stream:** leave out `--trigger-mode available_now`. The job then picks up new messages every 10 seconds until stopped (Ctrl+C).
- **Gold timing:** a Gold hour appears only after the 10-minute watermark passes it, so a short run doesn't write the last hours.

**Check [VM2 bash]:**

```bash
scripts/pull_output_minio.sh stream
python3 scripts/reconcile.py              # every message sent is in exactly one output: OK
python3 scripts/show_results.py
python3 scripts/evaluate_stream.py
```

### 12.4 Start the stream fresh

Reset Kafka and the checkpoints **together**, otherwise the checkpoints won't match the new topics.

**[VM2 bash]**:

```bash
~/kafka/bin/kafka-server-stop.sh; sleep 5; rm -rf ~/kafka-data
cd ~/kafka && bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/kraft/server.properties
KAFKA_HEAP_OPTS="-Xms512m -Xmx1g" bin/kafka-server-start.sh -daemon config/kraft/server.properties && cd -
mc rm --recursive --force local/lake/stream local/lake/checkpoints
rm -rf output/stream output/producer-stats
python3 scripts/create_topics.py
```

---

## 13. Daily routine and cost

### 13.1 Start a session

1. **[your machine]** `az vm start -g <RG> -n spark-worker` and `az vm start -g <RG> -n spark-master`.
2. If the public IPs changed, update `~/.ssh/config`. The private IPs stay the same if you made them static (section 1.3).
3. These start by themselves at boot:
   - the master (scheduled task);
   - the worker and MinIO (systemd services).
4. Open the tunnels (section 6) and check that the master UI shows VM2 ALIVE.
5. Kafka (if used) must be started by hand (section 12.1).

### 13.2 End a session

```bash
az vm deallocate -g <RG> -n spark-master
az vm deallocate -g <RG> -n spark-worker
```

- **Deallocate, don't just stop:** **deallocate** stops compute billing. Shutting down from inside the OS does not.
- **Disks still bill** while deallocated.
- **Data survives:** MinIO's data is on VM2's OS disk (or a data disk), so it survives.

### 13.3 Cost notes

- **Windows costs more:** the Windows VM costs more per hour than Ubuntu, because of the license.
- **No egress:** traffic between VMs in the same VNet isn't charged. Only data leaving Azure is, such as `scp` to your machine, which is small for Gold.
- **Visual Studio subscription credit:** it has a spending limit. When the monthly credit runs out, the VMs are stopped and you aren't billed. Your disks and data are kept for a while, so check Microsoft's current terms. The credit renews each billing month.
- **When finished for good:** `az group delete -n <RG>` removes everything.

---

## 14. Demos on this setup

`DEMO.md` lists the concepts to demo. On SetupF:

- **Distributed work:** the Executors tab shows the driver on VM1 and executors on VM2, and wide stages show shuffle read/write.
- **Shuffle partitions and AQE:** change `batch.shuffle_partitions` in VM1's `config.yaml` between runs, and compare `gold/batch_run_metrics`.
- **Silver versus Gold:** query a KPI from Silver, then from Gold (DuckDB on VM2 or VM1), and compare the times.
- **Scaling and fault tolerance** (demos 5, 9, 10) need a second worker:
  1. Create a third Ubuntu VM like VM2, without MinIO.
  2. Repeat sections 5 and 8.3 on it.
  3. Raise `TOTAL_CORES` in `.env.setupF`.

  Its executors read MinIO on VM2 over the VNet.

---

## 15. Troubleshooting

### SSH and network

| Symptom | Fix |
|---|---|
| `Permissions 0664 ... are too open` | `chmod 400 ~/.ssh/<key>.pem` |
| SSH times out | NSG port 22 rule missing or wrong source IP; VM stopped; public IP changed |
| SSH to VM1 refused | `sshd` not running: rerun section 2.2 |
| VM1 password works, key doesn't | Key not in `administrators_authorized_keys`, or `icacls` not run (section 2.2) |
| Tunnel page doesn't load | Tunnel to the wrong IP (use `<MASTER_IP>` for VM1's UIs), or the service isn't running |

### Spark cluster

| Symptom | Fix |
|---|---|
| Worker not registering | Wrong `<MASTER_IP>` in the service; master task not running (`C:\spark-logs\master.log`); Windows Firewall rule for 7077 missing; `journalctl -u spark-worker` |
| Executors start then exit, "connection refused" to the driver | Windows Firewall blocking 40000-40002; `spark.driver.host` must be `<MASTER_IP>` |
| `Initial job has not accepted any resources` | No worker ALIVE, or `EXECUTOR_MEMORY`/`EXECUTOR_CORES` above the worker's 4g / 3 cores |
| `Cannot run program "C:\...python.exe"` on an executor | Started without `run_on_cluster.ps1`, or `spark.pyspark.python` isn't `python3` |
| `Python in worker has different version` | VM1 and VM2 must both be Python 3.12 |
| `JAVA_HOME is not set` in the master task | Variables set per user: set them machine-wide (section 3) and reboot |
| `Could not locate executable ...winutils.exe` / `HADOOP_HOME unset` | winutils missing or the session wasn't reopened; also try copying `hadoop.dll` to `C:\Windows\System32` |
| `UnsatisfiedLinkError ... NativeIO$Windows` | `hadoop.dll` missing or from another Hadoop version: use 3.3.x |
| Script "cannot be loaded because running scripts is disabled" | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned -Force`, or `powershell -ExecutionPolicy Bypass -File scripts\run_on_cluster.ps1 ...` |
| Job dies when SSH disconnects | Run it as a scheduled task (section 10.3) |
| `OutOfMemoryError` on the driver | Avoid large `collect()`; raise `DRIVER_MEMORY` only if VM1 has free RAM |
| `OutOfMemoryError` / executors killed on VM2 | Lower `EXECUTOR_MEMORY` to 2g, stop Kafka, or process fewer months |

### MinIO and S3A

| Symptom | Fix |
|---|---|
| `mc`: "request signature ... does not match" | Section 7.6 |
| `No FileSystem for scheme "s3a"` / `ClassNotFoundException ...S3AFileSystem` | Job not started through `run_on_cluster.ps1` (no packages), or VM1 can't reach Maven Central |
| `NoSuchMethodError` / `ClassNotFoundException` in S3A classes | `hadoop-aws` version differs from Spark's Hadoop (3.3.4) |
| Connection refused / timeout to `<VM2_IP>:9000` | MinIO not running, wrong IP in `S3_ENDPOINT`, `ufw` active on VM2 |
| `403` / `SignatureDoesNotMatch` from Spark | `S3_ACCESS_KEY` / `S3_SECRET_KEY` in `.env.setupF` differ from `/etc/default/minio` |
| `UnknownHostException: bronze.<ip>` | Path-style access off: `S3_ENDPOINT` wasn't set when the job started |
| `NoSuchBucket` | `mc mb local/bronze local/lake` |
| MinIO service won't start | `journalctl -u minio -n 50`; folder not owned by `minio-user`; password shorter than 8 characters |

### Project

| Symptom | Fix |
|---|---|
| `PATH_NOT_FOUND ... taxi_zone_lookup.csv` | Land with `land_data_minio.sh`, which adds `--zones` |
| A file `failed` with `not found` | That month isn't in `bronze`: land it |
| `No module named 'distutils'` or `numpy` in the ML job | `pip install setuptools==80.9.0 numpy==2.2.6` on VM2 (`--break-system-packages`) and on VM1 |
| `reconcile.py`: no `batch_run_log` | Pull `batch/silver` first (section 11.1) |
| `reconcile.py`: source file not found | `data/` on VM2 lacks that month: land it |
| `reconcile.py`: MISMATCH | A job was interrupted: rerun the month |
| `No space left on device` on VM2 | `df -h /`; clear `/mnt/spark-tmp`; delete reconciled months from `data/` and `output/`; `mc rm --recursive --force local/lake/<folder>`; or add a data disk |
| Stream: `Failed to find data source: kafka` | Start it through `run_on_cluster.ps1 stream` |
| Producer waits for Kafka then gives up | Kafka not running, wrong `KAFKA_BOOTSTRAP`, or topics not created |

---

## 16. What changed in the project for SetupF

| File | Change |
|---|---|
| `spark/storage.py` | `join`, `exists`, `delete`, `mkdirs`, `read_text`, `write_text` through Hadoop's FileSystem, so paths can be local, `abfss://` or `s3a://` |
| `spark/batch_job.py`, `ml_job.py`, `stream_job.py` | Use those helpers; paths are strings |
| `spark/stream_job.py` `build_spark` | With `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`: the S3A endpoint and keys, path-style access, SSL only for `https`. `SPARK_PACKAGES` adds Maven packages |
| `scripts/run_on_cluster.ps1` | VM1: runs `batch`, `ml` or `stream` on the cluster with the settings from `.env.setupF` |
| `scripts/land_data_minio.sh` | VM2: download, then `mc mirror data → local/bronze` |
| `scripts/pull_output_minio.sh` | VM2: `mc mirror local/lake/<folder> → output/<folder>` |
| `scripts/create_topics.py` | Creates the Kafka topics from `config.yaml` (moved from `kafka-init/`) |
| `setupF.env.example`, `.gitignore` | The settings template; `.env.setupF` is ignored by git |
| `config.yaml` | Paths relative to the project folder; one Kafka address (`localhost:9092`, overridden by `KAFKA_BOOTSTRAP`) |
| Removed | Docker: `docker-compose.yml`, `.env`, the Dockerfiles, `kafka-init/` |

---

## 17. What was checked

On the laptop, with a temporary synthetic zone file and no downloaded data:

| Check | Result |
|---|---|
| Full test suite, except the dashboard test (`streamlit` not installed on the laptop) | 79 passed, 12 skipped, 1 failed |
| `build_spark` with `S3_ENDPOINT`: endpoint, path-style and SSL settings reach the Spark conf; `fs.s3a.impl` is `S3AFileSystem` | OK |
| `bash -n` of the two VM2 scripts | OK |

The one failure, `test_kpi_queries_answer_on_duckdb_and_spark_sql`, expects a 24-month batch range while `config.yaml`
has 3 months. It isn't caused by these changes.

**Not checked:**
- **MinIO:** any real `s3a://` read or write.
- **`run_on_cluster.ps1`:** PowerShell isn't installed on the laptop.
- **The rest of the setup:** the `mc` commands, the Windows steps, the systemd units, the cluster, Kafka, and the ML job on the cluster.
