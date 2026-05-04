# BMC / IPMI Out-of-Band Access via Tailscale

> **Why this exists.** On 2026-05-02, sshd on `grid-svr` was OOM-killed and Anik
> had to physically walk to the machine to power-cycle it. This directory is
> the "never get stranded again" backstop: an always-on path to the
> motherboard's BMC (the IPMI / iDRAC / iLO / IPMIView / "out of band"
> controller — same thing, different vendor names) so that even if Linux on
> grid-svr is dead, you can still reach power, KVM, and serial console from
> anywhere on the Tailnet.
>
> The BMC runs on its own embedded chip, drawing standby power, and is
> independent of the host OS. As long as the wall socket has power and the
> Ethernet cable is plugged in, the BMC is reachable.

---

## tl;dr — the 60-second version

1. Find the BMC's LAN IP (router DHCP table, see below).
2. Fill in `bmc-env.example` → save as `bmc-env`.
3. Pick **Approach B** (a separate always-on tiny box runs Tailscale subnet
   routing). This is the one that survives `grid-svr` being a brick.
4. Approve the subnet route at https://login.tailscale.com/admin/machines.
5. From any Tailnet device: `ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS chassis power status` should report `Chassis Power is on`.
6. Bookmark `cheatsheet.md` for the next 3am panic.

---

## Step 0 — Find your BMC's LAN IP

The BMC has its own MAC address and gets its own DHCP lease, separate from
grid-svr's host OS NIC. Some motherboards share the physical port, some have
a dedicated "IPMI" RJ-45.

**How to find it:**

- **Router DHCP leases**: log into your router's admin UI and look for
  hostnames like `BMC`, `IPMI`, `iDRAC`, `iLO`, or the motherboard vendor name
  (`SUPERMICRO`, `ASRockRack`, `ASPEED`). MAC OUI lookups for ASPEED
  (`AC:1F:6B`, `00:25:90`) often surface BMCs.
- **From grid-svr while it's still alive**: `sudo ipmitool lan print 1`
  prints the BMC's IP address, MAC, and gateway (channel `1` is most common
  but try `2`, `3` if `1` is empty).
- **Vendor scan tools**: Supermicro's `IPMICFG`, Dell's `racadm`, HP's
  `hponcfg`. Last resort.
- **Physical**: the IP is sometimes printed on a sticker, or shown on POST.

Once found, also note the BMC's **MAC** so you can pin a static DHCP
reservation at the router — you do NOT want this address moving around.

---

## Step 1 — Default credentials (CHANGE THEM IMMEDIATELY)

Try these against the BMC web UI (`https://<BMC_IP>/`) and via ipmitool.

| Vendor | User | Password |
|---|---|---|
| Supermicro (older) | `ADMIN` | `ADMIN` |
| Supermicro (newer, ~2020+) | `ADMIN` | unique sticker on motherboard |
| Dell iDRAC | `root` | `calvin` |
| HPE iLO | `Administrator` | unique sticker |
| ASRock Rack | `admin` | `admin` |
| Lenovo/IBM IMM | `USERID` | `PASSW0RD` (zero, not O) |
| ASUS ASMB | `admin` | `admin` |

> **CRITICAL**: BMCs are root-equivalent. They can power on/off the box,
> mount remote ISOs, and serve a full keyboard/video/mouse session. A BMC
> with default credentials on a routable network is one of the worst
> security postures in computing. **Change the password before doing
> anything else**, and never expose port 443/623 to the public internet.

After login: navigate to user management → change `ADMIN`/`root`/whoever's
password to a strong unique value. Store it in your password manager. Update
`bmc-env` to match.

---

## Step 2 — Pick an approach

### Approach A — Tailscale subnet routing on `grid-svr` itself

`grid-svr` runs Tailscale (already does, as `100.75.185.36`) and advertises
the BMC's LAN subnet. Anik approves the route once.

```bash
# On grid-svr, as root or with sudo:
sudo tailscale up \
  --advertise-routes=192.168.1.0/24 \
  --accept-routes \
  --reset=false
```

**Pros**: zero new hardware, ~3 minutes of work.

**Limitation**: when `grid-svr` is down, the BMC is unreachable too — which
is exactly the failure mode this whole exercise is trying to prevent. Use
this as a stopgap or as a redundant second path, never as the primary.

### Approach B — Tailscale on a separate always-on tiny box (RECOMMENDED)

A second device on the same LAN as the BMC runs Tailscale and advertises the
BMC's subnet (or just the BMC's `/32`). When `grid-svr` is bricked, this
gateway box is still up, the BMC is still up, and the path between them is
still up.

**Eligible hardware** (anything with a wired NIC and 24/7 uptime):

- Raspberry Pi 4 / 5 (cheapest, ~$45, low power draw)
- Existing OpenWrt or pfSense router (Tailscale runs as a package)
- Synology / QNAP NAS (Tailscale is in the package center)
- Any Mini-PC, Intel NUC, old laptop with the lid closed
- Even a phone running `Tailscale` + `Termux`, in a pinch

Setup on the gateway box (any Debian-flavored Linux):

```bash
# 1. Install Tailscale
curl -fsSL https://tailscale.com/install.sh | sh

# 2. Enable IP forwarding (required for subnet routing)
echo 'net.ipv4.ip_forward = 1' | sudo tee -a /etc/sysctl.d/99-tailscale.conf
echo 'net.ipv6.conf.all.forwarding = 1' | sudo tee -a /etc/sysctl.d/99-tailscale.conf
sudo sysctl -p /etc/sysctl.d/99-tailscale.conf

# 3. Bring it up advertising the BMC subnet
sudo tailscale up \
  --advertise-routes=192.168.1.0/24 \
  --accept-routes \
  --hostname=bmc-gateway

# 4. Confirm
tailscale status
```

**Pros**: survives `grid-svr` going to the moon. Cheap. Doubles as a general
LAN-side bastion (you can also put `pihole`, monitoring, backups on it).

**Cons**: one more device to maintain. Pick one with auto-updates (Pi OS
unattended-upgrades, OpenWrt sysupgrade) so it doesn't rot.

### Recommendation

Do **both**. Approach B is the lifeline; Approach A is the redundant primary
when grid-svr is healthy. Tailscale will pick one route and the other is
warm standby — losing either does not break BMC access.

---

## Step 3 — Approve the subnet route in the Tailscale admin

1. Go to https://login.tailscale.com/admin/machines
2. Find the gateway node (`grid-svr` for Approach A, `bmc-gateway` for
   Approach B). It will show "Subnets — 1 awaiting approval" or similar.
3. Click the `…` menu → **Edit route settings…**
4. Toggle the BMC subnet ON. Save.
5. (Optional but recommended) Disable key expiry on this node so it never
   silently goes offline at 3am.

---

## Step 4 — Verify

From any other Tailnet device (your laptop, phone, etc.):

```bash
# 1. Ping the BMC
ping -c 3 $BMC_IP

# 2. Hit the web UI (will be a vendor splash page)
curl -k -I https://$BMC_IP/

# 3. ipmitool over LAN+
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS chassis status

# Expected output:
# System Power         : on
# Power Overload       : false
# Main Power Fault     : false
# ...
```

If `chassis status` works, you're done. Bookmark `cheatsheet.md` and walk
away.

---

## Files in this directory

- `README.md` — this document
- `bmc-env.example` — variables to fill in (copy to `bmc-env`)
- `install-ipmitool.sh` — apt-installs `ipmitool`/`freeipmi-tools`, has a
  `--check` mode that probes the BMC
- `grid-bmc-tunnel.service` — systemd unit (Approach A) that re-asserts the
  Tailscale subnet route advertisement on boot
- `healthcheck.sh` — cron-friendly probe; logs warning if BMC unreachable
- `cheatsheet.md` — single-page "sshd is dead, what do I do" runbook

---

## Security recap (read again, then read once more)

- **Never** expose the BMC's web UI or port 623 to the public internet.
  Tailscale-only.
- **Always** change default credentials before completing setup.
- **Pin** the BMC's MAC to a static DHCP reservation on the router so the
  IP doesn't drift.
- **Update** BMC firmware ~yearly. Vendor sites (Supermicro, Dell, HPE)
  publish security advisories. Search history is full of pre-auth RCEs in
  BMC firmware.
- **Audit**: anyone with Tailnet access + the BMC password can power-cycle
  the box and mount arbitrary boot media. Treat that password like a
  root password.
