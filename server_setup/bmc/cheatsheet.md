# BMC PANIC CHEATSHEET — sshd is dead, what do I do

> One page. Paste it into a 3am panic. All commands assume `bmc-env` is
> sourced (`set -a; . /etc/grid/bmc-env; set +a`) so `$BMC_IP`, `$BMC_USER`,
> `$BMC_PASS` are in scope. If they aren't, expand by hand.

## 0. Confirm you can reach the BMC

```bash
ping -c 3 $BMC_IP
curl -k -I https://$BMC_IP/
```

If both fail, the Tailscale subnet route is down. Check
https://login.tailscale.com/admin/machines and confirm the gateway
(`bmc-gateway` or `grid-svr`) shows the BMC subnet as approved + active.

## 1. Power state (read-only, safe to run anytime)

```bash
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS chassis power status
# -> Chassis Power is on   (or off)
```

## 2. Power cycle the box

| Goal | Command |
|---|---|
| Soft shutdown (ACPI, OS-aware) | `ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS chassis power soft` |
| Graceful reboot | `... chassis power reset` |
| HARD power cycle (off, then on) | `... chassis power cycle` |
| Force off (last resort) | `... chassis power off` |
| Force on | `... chassis power on` |

If sshd is OOM-killed but the box is otherwise responsive: try `power soft`
first, then `power reset`. If the kernel is wedged: `power cycle`.

## 3. Serial-Over-LAN (SoL) — see boot output, get a console

```bash
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS sol activate
# Exit with: ~.   (tilde, dot — just like ssh escape)
# If it hangs on exit, ~~. lets the outer ssh session catch it.
```

Use this to watch POST, GRUB, or kernel panics in real time. If you see
the GRUB menu, you can pick a recovery kernel.

If a previous session left SoL stuck: `... sol deactivate` first.

## 4. Sensors — is the box on fire?

```bash
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS sdr            # all sensors
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS sdr type temp   # CPU/board temps
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS sdr type fan    # fans
```

A dead fan + climbing CPU temp explains a lot of OOM-adjacent weirdness.

## 5. Event log (system event log / SEL)

```bash
ipmitool -I lanplus -H $BMC_IP -U $BMC_USER -P $BMC_PASS sel elist | tail -50
# Clear after triage: ipmitool ... sel clear
```

ECC errors, thermal trips, watchdog timeouts all show up here.

## 6. Browser — full KVM-over-IP

```
https://$BMC_IP/
```

Vendor splash page → log in → look for "iKVM/HTML5" or "Remote Console" or
"Virtual Console". You get keyboard, video, and mouse, plus virtual media
(mount an ISO from your laptop to boot from). This is the nuclear option —
if you can KVM, you can fix anything.

## 7. After grid-svr is back up

```bash
# On grid-svr:
sudo systemctl status sshd
journalctl -u sshd -b -1     # what did sshd say in the previous boot?
journalctl -k -b -1 | grep -i 'oom\|killed'   # OOM victims last boot

# Then make sure tailscaled came back too:
sudo tailscale status
sudo systemctl status tailscaled
```

## 8. Things to install on the BMC web UI WHILE YOU'RE LOGGED IN

(Do this once, not in a panic.)

- Change default credentials. Yes, again. Verify.
- Set NTP so SEL timestamps are real.
- Enable email alerts → your inbox, for fan/temp/power events.
- Disable any vendor-default user accounts you don't use (Supermicro often
  ships with a hidden second admin — kill it).
- Update BMC firmware if it's > 12 months old.

## 9. Don't

- Don't expose the BMC to the public internet. Tailnet only.
- Don't share `$BMC_PASS` over chat. It's root-equivalent forever.
- Don't `sel clear` before you've actually read the SEL.
