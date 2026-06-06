# Bolt KV Store — Deployment Guide

## Local Docker (run the full cluster on your laptop)

### Prerequisites
- Docker Desktop installed

### Start the 3-node cluster
```bash
docker-compose up --build
```

You'll see all 3 nodes start, elect a leader, and begin heartbeating.

### Connect to the cluster
```bash
python cluster_client.py
```

### Stop the cluster
```bash
docker-compose down
```

---

## GCP Deployment (free tier — 3 e2-micro VMs)

### Why GCP?
- Free tier: 3 x e2-micro VMs (enough for 3 Raft nodes)
- Same infrastructure Google uses internally
- Shows on your resume that you've deployed on Google Cloud

### Step 1 — Create a GCP account
Go to https://cloud.google.com and create an account.
You get $300 free credits + always-free e2-micro VMs.

### Step 2 — Install Google Cloud CLI
```bash
# Download from https://cloud.google.com/sdk/docs/install
gcloud init
gcloud auth login
```

### Step 3 — Create 3 VMs
```bash
# Create the VMs (free tier)
for NODE in A B C; do
  gcloud compute instances create bolt-node-$NODE \
    --machine-type=e2-micro \
    --zone=us-central1-a \
    --image-family=debian-11 \
    --image-project=debian-cloud \
    --tags=bolt-kv
done
```

### Step 4 — Open firewall ports
```bash
gcloud compute firewall-rules create bolt-kv-ports \
  --allow=tcp:6379,tcp:6380,tcp:6381 \
  --target-tags=bolt-kv \
  --description="Bolt KV Store ports"
```

### Step 5 — Deploy to each VM
```bash
# SSH into each VM and run:
gcloud compute ssh bolt-node-A

# On the VM:
sudo apt-get update && sudo apt-get install -y python3 python3-pip git
git clone https://github.com/YOUR_USERNAME/bolt-kv.git
cd bolt-kv

# Start Node A (replace IPs with your actual VM IPs)
python3 raft_node.py --id A --port 6379 --host 0.0.0.0
```

### Step 6 — Update cluster configuration
Edit `raft_node.py` and `cluster_client.py` to use the real VM IPs
instead of `127.0.0.1`.

### Step 7 — Verify deployment
From your laptop:
```bash
python cluster_client.py  # update CLUSTER_NODES with real IPs
bolt-cluster> PING
  Node A: +PONG
  Node B: +PONG
  Node C: +PONG
bolt-cluster> STATS
```

---

## Architecture on GCP

```
Your Laptop
     │
     │ TCP
     ▼
┌────────────┐    Raft RPC    ┌────────────┐
│ Node A     │◄──────────────►│ Node B     │
│ e2-micro   │                │ e2-micro   │
│ port 6379  │                │ port 6380  │
└────────────┘                └────────────┘
      ▲                            ▲
      │         Raft RPC           │
      └──────────────┬─────────────┘
                     │
               ┌─────▼──────┐
               │ Node C     │
               │ e2-micro   │
               │ port 6381  │
               └────────────┘
```

---

## Resume talking points

- "Deployed a 3-node distributed KV store on Google Cloud Platform"
- "Used Docker Compose for local cluster orchestration"
- "Nodes communicate via custom TCP protocol with Raft consensus"
- "Cluster survives node failures — tested by killing one VM"
