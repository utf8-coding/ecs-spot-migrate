#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ECS 抢占式实例迁移脚本
流程: 选源实例 -> 打快照 -> 建镜像 -> 用镜像建新抢占式实例 -> 释放老实例 + 清理旧资源

 用法:
  python3 migrate_spot_instance.py                     # 正常运行
  python3 migrate_spot_instance.py --list              # 列出所有可用选项 (grep 可筛)
  python3 migrate_spot_instance.py --price             # 查报价: YAML 配置的当前抢占式价格
  python3 migrate_spot_instance.py --price -t ecs.sn1ne.xlarge               # 查报价: 指定规格
  python3 migrate_spot_instance.py --price -t ecs.sn1ne.xlarge -z cn-shenzhen-f  # 指定规格+可用区
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# --- dotenv (optional) -------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

# --- 3rd-party ---------------------------------------------------------
import yaml
from aliyunsdkcore.client import AcsClient
from aliyunsdkecs.request.v20140526 import (
    CreateImageRequest,
    CreateSnapshotRequest,
    DeleteImageRequest,
    DeleteInstanceRequest,
    DeleteSnapshotRequest,
    DescribeImagesRequest,
    DescribeInstancesRequest,
    DescribeSnapshotsRequest,
    RunInstancesRequest,
)

# ======================================================================
# CONSTANTS
# ======================================================================

REGION_ID = "cn-shenzhen"
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_DIR = SCRIPT_DIR / "config"
CSV_FILE = SCRIPT_DIR / "resources.csv"
TZ_UTC8 = timezone(timedelta(hours=8))

POLL_INTERVAL = 5          # 轮询间隔 (秒)
SNAPSHOT_TIMEOUT = 900     # 快照等待超时 (15 分钟)
IMAGE_TIMEOUT = 900        # 镜像等待超时 (15 分钟)
INSTANCE_TIMEOUT = 300     # 实例启动超时 (5 分钟)

# interrupt-friendly sleep: 分小段 sleep 让 Ctrl+C 秒级响应
def _interruptible_sleep(seconds: int):
    time.sleep(seconds)

# 断点状态常量
CK_SNAPSHOT_CREATING = "snapshot_creating"
CK_SNAPSHOT_DONE = "snapshot_done"
CK_IMAGE_CREATING = "image_creating"
CK_IMAGE_DONE = "image_done"
CK_INSTANCE_CREATING = "instance_creating"
CK_INSTANCE_DONE = "instance_done"
CK_DONE = "done"

# interrupt 处理标记

# ======================================================================
# UTILITIES
# ======================================================================


def utc8_now() -> datetime:
    """返回 UTC+8 当前时间"""
    return datetime.now(TZ_UTC8)


def timestamp_str() -> str:
    return utc8_now().strftime("%Y-%m-%dT%H:%M:%S+08")


def name_timestamp() -> str:
    """生成 YY.MM.DD.HH.mm 格式时间戳"""
    return utc8_now().strftime("%y.%m.%d.%H.%M")


def generate_resource_name(source_instance_name: str) -> str:
    """从源实例名生成快照/镜像/新实例名: 去尾部数字 + 去尾部特殊字符 + .YY.MM.DD.HH.mm"""
    base = re.sub(r"\.\d{2}\.\d{2}\.\d{2}(\.\d{2}\.\d{2})?$", "", source_instance_name)
    base = base.rstrip("-_. ")
    if not base:
        base = "ecs"
    return f"{base}.{name_timestamp()}"


def parse_bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "").strip().lower()
    if val in ("1", "true", "yes", "y"):
        return True
    return default


def confirm(prompt: str, default_yes: bool = True) -> bool:
    """交互式确认"""
    suffix = "[Y/n]" if default_yes else "[y/N]"
    try:
        ans = input(f"\n{prompt} {suffix} ").strip().lower()
    except EOFError:
        return default_yes
    if not ans:
        return default_yes
    return ans in ("y", "yes")


def print_box(title: str, lines: List[str]):
    """终端友好展示"""
    width = max(max(len(l) for l in lines), len(title) + 4, 60)
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'-' * width}")
    for line in lines:
        print(f"  {line}")
    print(f"{'=' * width}")


def now_short() -> str:
    return utc8_now().strftime("%H:%M:%S")


def log(msg: str):
    print(f"[{now_short()}] {msg}")


# ======================================================================
# INTERRUPT HANDLING - 直接用 KeyboardInterrupt, 不用 signal handler
# (signal handler 会导致 input() 卡住不返回)
# ======================================================================

# ======================================================================
# ALIYUN CLIENT
# ======================================================================


class AliClient:
    """封装阿里云 ECS API 调用"""

    def __init__(self):
        ak_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "").strip()
        ak_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "").strip()
        if not ak_id or not ak_secret:
            log("错误: 未找到 AccessKey。请设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET")
            log("或者在脚本同目录创建 .env 文件 (参考 .env.example)")
            sys.exit(1)
        self.client = AcsClient(ak_id, ak_secret, REGION_ID)

    def _call(self, request, timeout: int = 30):
        """发送 API 请求并返回 JSON dict"""
        request.set_accept_format("json")
        request.add_query_param("RegionId", REGION_ID)  # 添加不覆盖已有参数
        response = self.client.do_action_with_exception(request)
        return json.loads(response.decode("utf-8"))

    # ----- DescribeInstances --------------------------------------------

    def list_spot_instances(self):
        """查询深圳区所有抢占式实例"""
        req = DescribeInstancesRequest.DescribeInstancesRequest()
        
        req.set_InstanceChargeType("PostPaid")
        req.set_PageSize(100)
        req.set_MaxResults(100)

        instances = []
        while True:
            resp = self._call(req)
            items = resp.get("Instances", {}).get("Instance", [])
            for inst in items:
                spot_strategy = inst.get("SpotStrategy", "NoSpot")
                if spot_strategy in ("SpotWithPriceLimit", "SpotAsPriceGo"):
                    instances.append(inst)

            nxt = resp.get("NextToken")
            if nxt:
                req.set_NextToken(nxt)
            else:
                break
        return instances

    def get_instance(self, instance_id: str) -> Optional[dict]:
        """查询单个实例详情"""
        req = DescribeInstancesRequest.DescribeInstancesRequest()
        
        req.set_InstanceIds(json.dumps([instance_id]))
        resp = self._call(req)
        items = resp.get("Instances", {}).get("Instance", [])
        return items[0] if items else None

    def get_instance_status(self, instance_id: str) -> str:
        """返回实例状态 Running / Stopped / Stopping / Pending 等"""
        inst = self.get_instance(instance_id)
        if inst:
            return inst.get("Status", "Unknown")
        return "NotFound"

    def get_system_disk_id(self, instance_id: str) -> Optional[str]:
        """从实例详情中提取系统盘 ID。DescribeInstances 可能不返回 (节省停机), fallback 到 DescribeDisks"""
        inst = self.get_instance(instance_id)
        if inst:
            disk_id = inst.get("SystemDisk", {}).get("DiskId", "") if inst.get("SystemDisk") else ""
            if disk_id:
                return disk_id

        # Fallback: 通过 DescribeDisks 查询
        from aliyunsdkecs.request.v20140526 import DescribeDisksRequest
        req = DescribeDisksRequest.DescribeDisksRequest()
        req.set_InstanceId(instance_id)
        resp = self._call(req)
        disks = resp.get("Disks", {}).get("Disk", [])
        for d in disks:
            if d.get("Type") == "system":
                return d.get("DiskId")
        return None

    # ----- Snapshot -----------------------------------------------------

    def create_snapshot(self, disk_id: str, snapshot_name: str) -> str:
        """创建快照, 返回 SnapshotId"""
        req = CreateSnapshotRequest.CreateSnapshotRequest()
        req.set_DiskId(disk_id)
        req.set_SnapshotName(snapshot_name)
        req.set_ClientToken(str(uuid.uuid4()))
        resp = self._call(req)
        return resp.get("SnapshotId", "")

    def get_snapshot(self, snapshot_id: str) -> Optional[dict]:
        """查询单个快照"""
        req = DescribeSnapshotsRequest.DescribeSnapshotsRequest()
        
        req.set_SnapshotIds(json.dumps([snapshot_id]))
        resp = self._call(req)
        items = resp.get("Snapshots", {}).get("Snapshot", [])
        return items[0] if items else None

    def get_snapshot_status(self, snapshot_id: str) -> str:
        """返回快照状态 accomplishing / progressing / failed"""
        snap = self.get_snapshot(snapshot_id)
        if snap:
            return snap.get("Status", "Unknown")
        return "NotFound"

    def delete_snapshot(self, snapshot_id: str):
        """删除快照"""
        req = DeleteSnapshotRequest.DeleteSnapshotRequest()
        req.set_SnapshotId(snapshot_id)
        self._call(req)

    # ----- Image --------------------------------------------------------

    def create_image(self, snapshot_id: str, image_name: str) -> str:
        """从快照创建镜像, 返回 ImageId"""
        req = CreateImageRequest.CreateImageRequest()
        
        req.set_SnapshotId(snapshot_id)
        req.set_ImageName(image_name)
        req.set_ClientToken(str(uuid.uuid4()))
        resp = self._call(req)
        return resp.get("ImageId", "")

    def get_image(self, image_id: str) -> Optional[dict]:
        """查询单个镜像"""
        req = DescribeImagesRequest.DescribeImagesRequest()
        
        req.set_ImageId(image_id)
        req.set_Status("Creating,Available,UnAvailable,CreateFailed")
        resp = self._call(req)
        items = resp.get("Images", {}).get("Image", [])
        return items[0] if items else None

    def get_image_status(self, image_id: str) -> str:
        """返回镜像状态 Available / Creating / UnAvailable / CreateFailed"""
        img = self.get_image(image_id)
        if img:
            return img.get("Status", "Unknown")
        return "NotFound"

    def delete_image(self, image_id: str, force: bool = False):
        """删除镜像; force=True 时强制删除即使有实例在使用"""
        req = DeleteImageRequest.DeleteImageRequest()
        
        req.set_ImageId(image_id)
        if force:
            req.set_Force(True)
        self._call(req)

    # ----- Spot Price ----------------------------------------------------

    def query_spot_price(self, instance_type: str, zone_id: str) -> Optional[Dict[str, Any]]:
        """查询指定规格/可用区的抢占式实例当前报价"""
        from aliyunsdkecs.request.v20140526 import DescribeSpotPriceHistoryRequest
        req = DescribeSpotPriceHistoryRequest.DescribeSpotPriceHistoryRequest()
        req.set_InstanceType(instance_type)
        req.set_ZoneId(zone_id)
        req.set_NetworkType("vpc")
        req.set_SpotDuration(0)
        req.set_accept_format("json")

        resp = self._call(req)
        prices = resp.get("SpotPrices", {}).get("SpotPriceType", [])
        if not prices:
            return None
        # 取最新一条
        prices.sort(key=lambda p: p.get("Timestamp", ""), reverse=True)
        return prices[0]

    # ----- Instance -----------------------------------------------------

    def run_instances(self, params: Dict[str, Any]) -> str:
        """创建实例, 返回 InstanceId (单台)"""
        req = RunInstancesRequest.RunInstancesRequest()
        
        req.set_ImageId(params["ImageId"])
        req.set_InstanceType(params["InstanceType"])
        req.set_VSwitchId(params["VSwitchId"])
        req.set_ZoneId(params.get("ZoneId", ""))
        req.set_InstanceChargeType("PostPaid")
        req.set_Amount(1)
        req.set_PasswordInherit(True)

        # 抢占式参数
        spot_strategy = params.get("SpotStrategy", "SpotAsPriceGo")
        req.set_SpotStrategy(spot_strategy)
        if spot_strategy != "NoSpot":
            req.set_SpotDuration(params.get("SpotDuration", 0))
            behavior = params.get("SpotInterruptionBehavior", "Stop")
            req.set_SpotInterruptionBehavior(behavior)

        # 网络
        req.set_InternetMaxBandwidthOut(params.get("InternetMaxBandwidthOut", 0))
        req.set_InternetChargeType(params.get("InternetChargeType", "PayByTraffic"))

        # 系统盘
        req.set_SystemDiskSize(params.get("SystemDisk.Size", 40))
        sys_category = params.get("SystemDisk.Category", "")
        if sys_category:
            req.set_SystemDiskCategory(sys_category)

        # 安全组
        sg_ids = params.get("SecurityGroupIds", [])
        if isinstance(sg_ids, str):
            sg_ids = [sg_ids]
        if len(sg_ids) == 1:
            req.set_SecurityGroupId(sg_ids[0])
        elif len(sg_ids) > 1:
            req.set_SecurityGroupIdss(sg_ids)

        # 实名/描述
        if params.get("InstanceName"):
            req.set_InstanceName(params["InstanceName"])
        if params.get("HostName"):
            req.set_HostName(params["HostName"])
        if params.get("Description"):
            req.set_Description(params["Description"])

        # 密钥对 / RAM 角色
        if params.get("KeyPairName"):
            req.set_KeyPairName(params["KeyPairName"])
        if params.get("RamRoleName"):
            req.set_RamRoleName(params["RamRoleName"])
        if params.get("ResourceGroupId"):
            req.set_ResourceGroupId(params["ResourceGroupId"])

        # ClientToken 幂等
        req.set_ClientToken(str(uuid.uuid4()))

        resp = self._call(req)
        ids = resp.get("InstanceIdSets", {}).get("InstanceIdSet", [])
        if ids:
            return ids[0]
        return ""

    def delete_instance(self, instance_id: str):
        """释放实例 (按量付费)"""
        req = DeleteInstanceRequest.DeleteInstanceRequest()
        req.set_InstanceId(instance_id)
        req.set_Force(True)
        self._call(req)

    # ----- Polling helpers -----------------------------------------------

    def poll_snapshot(self, snapshot_id: str) -> bool:
        """轮询直到快照 accomplished; 返回成功/失败"""
        log(f"    等待快照 {snapshot_id} 完成...")
        start = time.time()
        while time.time() - start < SNAPSHOT_TIMEOUT:
            status = self.get_snapshot_status(snapshot_id)
            log(f"      快照状态: {status}")
            if status == "accomplished":
                return True
            if status == "failed":
                log(f"    错误: 快照创建失败")
                return False
            if status == "NotFound":
                log(f"    错误: 快照不存在 (可能已被删除)")
                return False
            _interruptible_sleep(POLL_INTERVAL)
        log(f"    错误: 快照创建超时 ({SNAPSHOT_TIMEOUT}s)")
        return False

    def poll_image(self, image_id: str) -> bool:
        """轮询直到镜像 Available; 返回成功/失败"""
        log(f"    等待镜像 {image_id} 可用...")
        start = time.time()
        while time.time() - start < IMAGE_TIMEOUT:
            status = self.get_image_status(image_id)
            log(f"      镜像状态: {status}")
            if status == "Available":
                return True
            if status in ("CreateFailed",):
                log(f"    错误: 镜像创建失败")
                return False
            if status == "NotFound":
                log(f"    错误: 镜像不存在 (可能已被删除)")
                return False
            _interruptible_sleep(POLL_INTERVAL)
        log(f"    错误: 镜像创建超时 ({IMAGE_TIMEOUT}s)")
        return False

    def poll_instance_running(self, instance_id: str) -> bool:
        """轮询直到实例 Running"""
        log(f"    等待实例 {instance_id} 启动...")
        start = time.time()
        while time.time() - start < INSTANCE_TIMEOUT:
            status = self.get_instance_status(instance_id)
            log(f"      实例状态: {status}")
            if status == "Running":
                return True
            if status in ("Stopped", "Stopping"):
                log(f"      实例已停止, 等待中...")
            _interruptible_sleep(POLL_INTERVAL)
        log(f"    错误: 实例启动超时 ({INSTANCE_TIMEOUT}s)")
        return False


# ======================================================================
# YAML CONFIG
# ======================================================================

def load_yaml_configs() -> List[Tuple[str, str, Dict]]:
    """
    扫描 config/ 目录, 返回 [(filename, config_raw_dict), ...]
    """
    configs = []
    for p in sorted(CONFIG_DIR.glob("*.yaml")):
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        configs.append((p.name, data))
    # 也支持 .yml
    for p in sorted(CONFIG_DIR.glob("*.yml")):
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        configs.append((p.name, data))
    if not configs:
        log("错误: config/ 目录下没有找到任何 .yaml 文件, 请先创建")
        sys.exit(1)
    return configs


# ======================================================================
# SOURCE INSTANCE CONFIG EXTRACTION
# ======================================================================

def extract_source_config(source_inst: dict) -> Dict[str, Any]:
    """从源实例提取可继承的配置参数"""
    cfg = {}

    # VPC/交换机/可用区
    vpc = source_inst.get("VpcAttributes", {})
    if vpc:
        cfg["VSwitchId"] = vpc.get("VSwitchId", "")
        cfg["VpcId"] = vpc.get("VpcId", "")

    cfg["ZoneId"] = source_inst.get("ZoneId", "")

    # 安全组
    sg_list = source_inst.get("SecurityGroupIds", {}).get("SecurityGroupId", [])
    cfg["SecurityGroupIds"] = list(sg_list)

    # 密钥对 / RAM 角色
    kpn = source_inst.get("KeyPairName", "")
    if kpn:
        cfg["KeyPairName"] = kpn
    rrn = source_inst.get("RamRoleName", "")
    if rrn:
        cfg["RamRoleName"] = rrn

    # 资源组
    rg = source_inst.get("ResourceGroupId", "")
    if rg:
        cfg["ResourceGroupId"] = rg

    # 系统盘
    sys_disk = source_inst.get("SystemDisk", {})
    cfg["SystemDisk.Category"] = sys_disk.get("Category", "cloud_efficiency")

    # 主机名/描述 (参考)
    cfg["description"] = source_inst.get("Description", "")

    return cfg


# ======================================================================
# MERGE CONFIG
# ======================================================================

def merge_config(source_cfg: dict, yaml_cfg: dict, resource_name: str, image_id: str) -> Dict[str, Any]:
    """
    合并配置: YAML 覆盖源实例继承参数, 其余继承
    返回 RunInstances 所需的参数字典
    """
    merged = {}

    # 先继承
    merged["VSwitchId"] = source_cfg.get("VSwitchId", "")
    merged["ZoneId"] = source_cfg.get("ZoneId", "")
    merged["SecurityGroupIds"] = source_cfg.get("SecurityGroupIds", [])
    merged["KeyPairName"] = source_cfg.get("KeyPairName", "")
    merged["RamRoleName"] = source_cfg.get("RamRoleName", "")
    merged["ResourceGroupId"] = source_cfg.get("ResourceGroupId", "")
    merged["SystemDisk.Category"] = source_cfg.get("SystemDisk.Category", "cloud_efficiency")
    merged["Description"] = source_cfg.get("description", "")

    # YAML 覆盖 (扁平化映射)
    yaml_field_map = {
        "instance_type": "InstanceType",
        "spot_strategy": "SpotStrategy",
        "spot_duration": "SpotDuration",
        "spot_interruption_behavior": "SpotInterruptionBehavior",
        "internet_max_bandwidth_out": "InternetMaxBandwidthOut",
        "internet_charge_type": "InternetChargeType",
        "disk_size": "SystemDisk.Size",
        "system_disk_category": "SystemDisk.Category",
        "system_disk_size": "SystemDisk.Size",
        "vswitch_id": "VSwitchId",
        "zone_id": "ZoneId",
        "security_group_id": "SecurityGroupId",
        "security_group_ids": "SecurityGroupIds",
        "key_pair_name": "KeyPairName",
        "ram_role_name": "RamRoleName",
        "resource_group_id": "ResourceGroupId",
        "instance_name": "InstanceName",
        "host_name": "HostName",
        "description": "Description",
    }

    for yk, v in yaml_cfg.items():
        if yk in ("name",):
            continue  # 仅展示用
        target_key = yaml_field_map.get(yk, yk)
        merged[target_key] = v

    # 固定覆盖
    merged["ImageId"] = image_id
    merged["InstanceName"] = resource_name
    merged["HostName"] = resource_name

    return merged


# ======================================================================
# CSV / CHECKPOINT
# ======================================================================

CSV_HEADER = [
    "run_id", "timestamp", "checkpoint", "snapshot_id", "image_id",
    "source_instance_id", "source_disk_id", "new_instance_id", "yaml_config", "resource_name",
]


def csv_read_all() -> List[Dict]:
    """读取 CSV 所有行"""
    if not CSV_FILE.exists():
        return []
    rows = []
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def csv_write_row(run_id: str, checkpoint: str, **kwargs):
    """追加或更新一行 (按 run_id 归并)"""
    rows = csv_read_all()
    found = False
    for r in rows:
        if r.get("run_id") == run_id:
            r["timestamp"] = timestamp_str()
            r["checkpoint"] = checkpoint
            for k, v in kwargs.items():
                r[k] = v
            found = True
            break
    if not found:
        row = {
            "run_id": run_id,
            "timestamp": timestamp_str(),
            "checkpoint": checkpoint,
            "snapshot_id": "",
            "image_id": "",
            "source_instance_id": "",
            "source_disk_id": "",
            "new_instance_id": "",
            "yaml_config": "",
            "resource_name": "",
        }
        row.update(kwargs)
        rows.append(row)

    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def csv_get_latest() -> Optional[Dict]:
    """获取 CSV 最后一行 (最近一次运行)"""
    rows = csv_read_all()
    if not rows:
        return None
    return rows[-1]


def csv_remove_run(run_id: str):
    """删除指定 run_id 的行"""
    rows = csv_read_all()
    rows = [r for r in rows if r.get("run_id") != run_id]
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerows([row])


def csv_keep_only(run_ids: List[str]):
    """只保留指定 run_id 的行"""
    rows = csv_read_all()
    keep_set = set(run_ids)
    rows = [r for r in rows if r.get("run_id") in keep_set]
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerows([row])


# ======================================================================
# CHECKPOINT RECOVERY
# ======================================================================

def handle_resume(client: AliClient) -> Optional[Tuple[str, Dict]]:
    """
    检测是否有未完成断点; 若有则询问用户恢复/丢弃
    返回 (run_id, row_dict) 或 None (从头开始)
    返回 None 时同时清理旧行
    """
    latest = csv_get_latest()
    if latest is None:
        return None
    cp = latest.get("checkpoint", "")
    if cp == CK_DONE:
        return None  # 上次已完成, 正常开始

    run_id = latest["run_id"]
    log(f"\n发现未完成的断点记录:")
    log(f"  run_id     = {run_id}")
    log(f"  时间       = {latest['timestamp']}")
    log(f"  断点       = {cp}")
    log(f"  快照ID     = {latest.get('snapshot_id', '')}")
    log(f"  镜像ID     = {latest.get('image_id', '')}")
    log(f"  源实例ID   = {latest.get('source_instance_id', '')}")
    log(f"  新实例ID   = {latest.get('new_instance_id', '')}")
    log(f"  配置       = {latest.get('yaml_config', '')}")

    # ----- 核验实际状态 ---------------------------------------------------
    log("\n正在核验云端实际状态...")
    checks_ok = True

    snapshot_id = latest.get("snapshot_id", "").strip()
    image_id = latest.get("image_id", "").strip()
    new_instance_id = latest.get("new_instance_id", "").strip()
    source_instance_id = latest.get("source_instance_id", "").strip()

    if snapshot_id:
        sstatus = client.get_snapshot_status(snapshot_id)
        log(f"  快照 {snapshot_id}: {sstatus}")
        if sstatus == "NotFound":
            checks_ok = False
    else:
        log(f"  快照: (未记录)")

    if image_id:
        istatus = client.get_image_status(image_id)
        log(f"  镜像 {image_id}: {istatus}")
        if istatus == "NotFound":
            checks_ok = False
    else:
        log(f"  镜像: (未记录)")

    if new_instance_id:
        nstatus = client.get_instance_status(new_instance_id)
        log(f"  新实例 {new_instance_id}: {nstatus}")
        if nstatus == "NotFound":
            checks_ok = False
    else:
        log(f"  新实例: (未记录)")

    if source_instance_id:
        ostatus = client.get_instance_status(source_instance_id)
        log(f"  源实例 {source_instance_id}: {ostatus}")
        # NotFound 是预期状态: 如果 checkpoint >= instance_done, 老实例已被清理
        if ostatus == "NotFound" and cp not in (CK_INSTANCE_DONE, CK_DONE):
            checks_ok = False

    if not checks_ok:
        log("\n警告: 云端资源状态与断点记录不一致, 可能资源已被手动删除。")
        if not confirm("是否丢弃断点, 从头开始? (若非预期请选择 N 退出检查)", default_yes=True):
            log("用户取消。")
            sys.exit(0)
        csv_remove_run(run_id)
        return None

    # ----- 询问用户 -------------------------------------------------------
    ans = input("\n[R] 从断点恢复  [S] 丢弃从头开始  [Q] 退出\n请选择: ").strip().lower()
    if ans == "s":
        # 丢弃: 尝试清理已经创建的资源
        log("\n丢弃断点, 清理已创建资源...")
        if new_instance_id and client.get_instance_status(new_instance_id) != "NotFound":
            if confirm(f"是否释放已创建的新实例 {new_instance_id}?"):
                try:
                    client.delete_instance(new_instance_id)
                    log(f"  已释放 {new_instance_id}")
                except Exception as e:
                    log(f"  释放失败: {e}")
        # 注意: 快照和镜像暂不自动清理 (可能被其他依赖)
        csv_remove_run(run_id)
        return None
    elif ans == "q":
        log("用户退出。")
        sys.exit(0)
    else:
        log("\n从断点恢复...")
        return run_id, latest


# ======================================================================
# MAIN WORKFLOW
# ======================================================================


def step_select_source(client: AliClient) -> Dict:
    """Step 1-2: 显示抢占式实例列表, 用户选择, 返回实例详情"""
    log("\n查询深圳区抢占式实例...")
    instances = client.list_spot_instances()

    if not instances:
        log("未找到抢占式实例, 退出。")
        sys.exit(0)

    print(f"\n找到 {len(instances)} 台抢占式实例:\n")
    print(f"{'#':<4} {'实例ID':<24} {'实例名':<30} {'规格':<22} {'CPU':>4} {'内存(MiB)':>10} {'竞价策略':<20} {'状态':<10}")
    print("-" * 130)
    for i, inst in enumerate(instances, 1):
        iid = inst.get("InstanceId", "")
        name = inst.get("InstanceName", "")
        itype = inst.get("InstanceType", "")
        cpu = inst.get("Cpu", 0)
        mem = inst.get("Memory", 0)
        spot = inst.get("SpotStrategy", "NoSpot")
        status = inst.get("Status", "")
        print(f"{i:<4} {iid:<24} {name:<30} {itype:<22} {cpu:>4} {mem:>10} {spot:<20} {status:<10}")

    # 选择源实例
    while True:
        try:
            ans = input("\n请输入目标实例编号 (q=退出): ").strip().lower()
            if ans in ("q", "quit", "exit"):
                log("用户退出。")
                sys.exit(0)
            choice = int(ans)
            if 1 <= choice <= len(instances):
                break
        except ValueError:
            pass
        print("请输入有效的编号。")

    selected = instances[choice - 1]
    iid = selected["InstanceId"]
    log(f"已选择: {iid} ({selected.get('InstanceName', '')})")

    # 查看系统盘 (节省停机时 SystemDisk 为空, 走 DescribeDisks fallback)
    disk_id = client.get_system_disk_id(iid)
    if not disk_id:
        log("错误: 无法获取实例系统盘 ID")
        sys.exit(1)
    log(f"系统盘 ID: {disk_id}")
    return selected


def step_select_yaml() -> Tuple[str, Dict]:
    """选择 YAML 配置"""
    configs = load_yaml_configs()
    print(f"\n可用 YAML 配置:\n")
    for i, (fname, data) in enumerate(configs, 1):
        name = data.get("name", fname)
        print(f"  {i}. {name}  ({fname})")

    while True:
        try:
            ans = input(f"\n请选择配置 (1-{len(configs)}, q=退出): ").strip().lower()
            if ans in ("q", "quit", "exit"):
                log("用户退出。")
                sys.exit(0)
            choice = int(ans)
            if 1 <= choice <= len(configs):
                break
        except ValueError:
            pass
        print("请输入有效的编号。")

    return configs[choice - 1]


def step_merge_and_confirm(client: AliClient, source_inst: Dict, yaml_fname: str,
                             yaml_data: Dict, resource_name: str, image_id: str = "") -> Dict:
    """合并配置并展示给用户确认"""
    source_cfg = extract_source_config(source_inst)
    merged = merge_config(source_cfg, yaml_data, resource_name, image_id)

    print_box("合并后的实例配置", [
        f"YAML 配置:    {yaml_data.get('name', yaml_fname)} ({yaml_fname})",
        f"资源名称:     {resource_name}",
        f"实例规格:     {merged.get('InstanceType', 'N/A')}",
        f"镜像ID:       {merged.get('ImageId', '(待创建)')}",
        f"可用区:       {merged.get('ZoneId', 'N/A')}",
        f"交换机:       {merged.get('VSwitchId', 'N/A')}",
        f"安全组:       {merged.get('SecurityGroupIds', [])}",
        f"密钥对:       {merged.get('KeyPairName', '') or '(无)'}",
        f"RAM 角色:     {merged.get('RamRoleName', '') or '(无)'}",
        f"资源组:       {merged.get('ResourceGroupId', '') or '(默认)'}",
        f"系统盘类型:   {merged.get('SystemDisk.Category', 'N/A')}",
        f"系统盘大小:   {merged.get('SystemDisk.Size', 'N/A')} GiB",
        f"竞价策略:     {merged.get('SpotStrategy', 'N/A')}",
        f"SpotDuration: {merged.get('SpotDuration', 'N/A')}",
        f"中断行为:     {merged.get('SpotInterruptionBehavior', 'N/A')}",
        f"公网带宽上限: {merged.get('InternetMaxBandwidthOut', 'N/A')} Mbps",
        f"网络计费:     {merged.get('InternetChargeType', 'N/A')}",
        f"密码:         使用镜像预设密码 (PasswordInherit=true)",
    ])

    return merged


def step_create_snapshot(client: AliClient, disk_id: str, resource_name: str,
                          csv_row: Dict) -> Optional[str]:
    """Step 3a: 创建快照"""
    # 如果断点中已有 snapshot_id 且已完成, 跳过
    if csv_row.get("snapshot_id", "").strip() and csv_row.get("checkpoint", "") in (
            CK_SNAPSHOT_DONE, CK_IMAGE_CREATING, CK_IMAGE_DONE, CK_INSTANCE_CREATING, CK_INSTANCE_DONE):
        sid = csv_row["snapshot_id"]
        log(f"  断点恢复: 快照 {sid} 已创建, 验证状态...")
        status = client.get_snapshot_status(sid)
        log(f"    快照状态: {status}")
        if status == "accomplished":
            return sid
        elif status == "progressing":
            return sid  # 继续轮询
        else:
            log(f"    快照状态异常, 重新创建")
            # fall through

    log(f"  创建快照 (磁盘 {disk_id})...")
    sn_name = resource_name
    sid = client.create_snapshot(disk_id, sn_name)
    log(f"  快照已创建: {sid}")
    return sid


def step_create_image(client: AliClient, snapshot_id: str, resource_name: str,
                       csv_row: Dict) -> Optional[str]:
    """Step 3b: 从快照创建镜像"""
    if csv_row.get("image_id", "").strip() and csv_row.get("checkpoint", "") in (
            CK_IMAGE_DONE, CK_INSTANCE_CREATING, CK_INSTANCE_DONE):
        iid = csv_row["image_id"]
        log(f"  断点恢复: 镜像 {iid} 已创建, 验证状态...")
        status = client.get_image_status(iid)
        log(f"    镜像状态: {status}")
        if status == "Available":
            return iid
        elif status == "Creating":
            return iid
        else:
            log(f"    镜像状态异常, 重新创建")

    log(f"  从快照 {snapshot_id} 创建镜像...")
    img_name = resource_name
    iid = client.create_image(snapshot_id, img_name)
    log(f"  镜像已创建: {iid}")
    return iid


def step_create_instance(client: AliClient, merged_config: Dict,
                          csv_row: Dict) -> Optional[str]:
    """Step 4: 创建新抢占式实例"""
    if csv_row.get("new_instance_id", "").strip() and csv_row.get("checkpoint", "") in (
            CK_INSTANCE_DONE,):
        niid = csv_row["new_instance_id"]
        log(f"  断点恢复: 新实例 {niid} 已创建, 验证状态...")
        status = client.get_instance_status(niid)
        log(f"    实例状态: {status}")
        if status == "Running":
            return niid
        elif status in ("Starting", "Pending", "Stopped"):
            return niid
        else:
            log(f"    实例状态异常, 重新创建")

    log("  创建新抢占式实例...")
    niid = client.run_instances(merged_config)
    log(f"  新实例创建请求已发送: {niid}")
    return niid


def step_cleanup(client: AliClient, source_instance_id: str,
                  current_snapshot_id: str, current_image_id: str,
                  current_run_id: str):
    """
    Step 5: 释放老实例 + 清理旧快照/镜像
    """
    log("\n⚠️  清理阶段: 请勿中断，否则可能残留中间状态。")
    # 5a. 释放老实例
    status = client.get_instance_status(source_instance_id)
    if status not in ("NotFound",):
        log(f"  即将释放源实例 {source_instance_id} ...")
        if not confirm("确认释放源实例?", default_yes=True):
            log("  跳过释放源实例。")
        else:
            try:
                client.delete_instance(source_instance_id)
                log(f"  已释放源实例 {source_instance_id}")
            except Exception as e:
                log(f"  释放失败: {e}")

    # 5b. 清理旧快照和镜像
    log("\n  清理 CSV 中记录的历史快照和镜像 (跳过本次运行)...")
    rows = csv_read_all()
    to_delete_snapshots = []
    to_delete_images = []

    for row in rows:
        if row.get("run_id") == current_run_id:
            continue  # 跳过本次运行
        sid = row.get("snapshot_id", "").strip()
        iid = row.get("image_id", "").strip()
        if sid and sid != current_snapshot_id:
            to_delete_snapshots.append(sid)
        if iid and iid != current_image_id:
            to_delete_images.append(iid)

    # 先删镜像 (依赖快照), 再删快照
    if to_delete_images:
        log(f"  待删除镜像: {len(to_delete_images)} 个")
        for img_id in to_delete_images:
            try:
                status = client.get_image_status(img_id)
                if status != "NotFound":
                    client.delete_image(img_id, force=True)
                    log(f"    已删除镜像 {img_id}")
                else:
                    log(f"    镜像 {img_id} 不存在, 跳过")
            except Exception as e:
                err = str(e)
                if "NotFound" in err or "not found" in err.lower():
                    log(f"    镜像 {img_id} 不存在, 跳过")
                else:
                    log(f"    删除镜像 {img_id} 失败: {err[:100]}")

    if to_delete_snapshots:
        log(f"  待删除快照: {len(to_delete_snapshots)} 个")
        for sn_id in to_delete_snapshots:
            try:
                status = client.get_snapshot_status(sn_id)
                if status != "NotFound":
                    client.delete_snapshot(sn_id)
                    log(f"    已删除快照 {sn_id}")
                else:
                    log(f"    快照 {sn_id} 不存在, 跳过")
            except Exception as e:
                err = str(e)
                if "NotFound" in err or "not found" in err.lower():
                    log(f"    快照 {sn_id} 不存在, 跳过")
                else:
                    log(f"    删除快照 {sn_id} 失败: {err[:100]}")

    # 5c. CSV 只保留本次
    csv_keep_only([current_run_id])
    log(f"  CSV 已清理, 仅保留本次运行记录。")


# ======================================================================
# CLI 命令: --price  / --list
# ======================================================================

def cmd_price(client: AliClient):
    """查询所有 YAML 配置对应规格的抢占式报价"""
    configs = load_yaml_configs()
    if not configs:
        log("未找到 YAML 配置")
        return

    # 收集唯一种规格+可用区组合
    seen = set()
    print(f"\n{'规格':<28} {'可用区':<22} {'抢占价(元/h)':>14} {'原价(元/h)':>12} {'折扣':>8} {'抢占月估':>12}")
    print("-" * 100)
    for fname, data in configs:
        itype = data.get("instance_type", "")
        if not itype:
            continue
        # 用所有可用区查价 (不带 ZoneId 返回所有可用区结果)
        from aliyunsdkecs.request.v20140526 import DescribeSpotPriceHistoryRequest
        req = DescribeSpotPriceHistoryRequest.DescribeSpotPriceHistoryRequest()
        req.set_InstanceType(itype)
        req.set_NetworkType("vpc")
        req.set_SpotDuration(0)
        req.set_accept_format("json")

        try:
            resp = client._call(req)
            prices = resp.get("SpotPrices", {}).get("SpotPriceType", [])
        except Exception as e:
            log(f"  {itype}: 查询失败 - {e}")
            continue

        if not prices:
            print(f"{itype:<28} {'(无数据)':<22}")
            continue

        prices.sort(key=lambda p: p.get("Timestamp", ""), reverse=True)
        # 按可用区分组取最新
        zone_latest = {}
        for p in prices:
            z = p.get("ZoneId", "")
            if z not in zone_latest:
                zone_latest[z] = p

        for z, p in sorted(zone_latest.items()):
            sp = float(p.get("SpotPrice", 0))
            op = float(p.get("OriginPrice", 0)) or 0.001
            disc = (1 - sp / op) * 100
            monthly = sp * 24 * 30
            print(f"{itype:<28} {z:<22} {sp:>14.3f} {op:>12.3f} {disc:>7.0f}% {monthly:>11.1f}")
    print()

    # 如果用户指定了 -t, 只查那一个
def cmd_price_specific(client: AliClient, instance_type: str, zone_id: Optional[str]):
    """查询指定规格的报价"""
    from aliyunsdkecs.request.v20140526 import DescribeSpotPriceHistoryRequest
    req = DescribeSpotPriceHistoryRequest.DescribeSpotPriceHistoryRequest()
    req.set_InstanceType(instance_type)
    req.set_NetworkType("vpc")
    req.set_SpotDuration(0)
    req.set_accept_format("json")
    if zone_id:
        req.set_ZoneId(zone_id)

    try:
        resp = client._call(req)
        prices = resp.get("SpotPrices", {}).get("SpotPriceType", [])
    except Exception as e:
        log(f"查询失败: {e}")
        return

    if not prices:
        log(f"{instance_type} 暂无报价数据")
        return

    prices.sort(key=lambda p: p.get("Timestamp", ""), reverse=True)
    zone_latest = {}
    for p in prices:
        z = p.get("ZoneId", "")
        if z not in zone_latest:
            zone_latest[z] = p

    print(f"\n{'规格':<28} {'可用区':<22} {'抢占价(元/h)':>14} {'原价(元/h)':>12} {'折扣':>8} {'抢占月估':>12}")
    print("-" * 100)
    for z, p in sorted(zone_latest.items()):
        sp = float(p.get("SpotPrice", 0))
        op = float(p.get("OriginPrice", 0)) or 0.001
        disc = (1 - sp / op) * 100
        monthly = sp * 24 * 30
        print(f"{instance_type:<28} {z:<22} {sp:>14.3f} {op:>12.3f} {disc:>7.0f}% {monthly:>11.1f}")
    print()


def cmd_list(client: AliClient):
    """列出所有可用选项: 抢占式实例 + YAML 配置 (grep 友好)"""
    # YAML configs
    configs = load_yaml_configs()
    print("\n[YAML 配置]")
    for fname, data in configs:
        print(f"  config/{fname}  name={data.get('name','')}  type={data.get('instance_type','')}  spot={data.get('spot_strategy','')}  bw={data.get('internet_max_bandwidth_out','')}Mbps  disk={data.get('disk_size','')}G")

    # Spot instances
    print("\n[抢占式实例]")
    instances = client.list_spot_instances()
    if not instances:
        print("  (无)")
    for inst in instances:
        iid = inst.get("InstanceId", "")
        name = inst.get("InstanceName", "")
        itype = inst.get("InstanceType", "")
        cpu = inst.get("Cpu", 0)
        mem = inst.get("Memory", 0)
        spot = inst.get("SpotStrategy", "")
        status = inst.get("Status", "")
        zone = inst.get("ZoneId", "")
        disk_id = inst.get("SystemDisk", {}).get("DiskId", "") if inst.get("SystemDisk") else ""
        # 节省停机 fallback
        if not disk_id:
            disk_id = client.get_system_disk_id(iid) or "?"
        print(f"  {iid}  name={name}  type={itype}  cpu={cpu}  mem={mem}MiB  spot={spot}  status={status}  zone={zone}  disk={disk_id}")

    # CSV history (snapshots / images)
    rows = csv_read_all()
    if rows:
        print(f"\n[CSV 历史 ({len(rows)} 行)]")
        for r in rows:
            cp = r.get("checkpoint", "")
            print(f"  run={r.get('run_id','')}  cp={cp}  snap={r.get('snapshot_id','')}  img={r.get('image_id','')}  src={r.get('source_instance_id','')}  new={r.get('new_instance_id','')}  cfg={r.get('yaml_config','')}  ts={r.get('timestamp','')}")

    print()


def parse_args():
    parser = argparse.ArgumentParser(description="ECS 抢占式实例迁移脚本")
    parser.add_argument("--price", action="store_true", help="查询抢占式报价")
    parser.add_argument("-t", "--type", dest="instance_type", help="指定实例规格 (配合 --price)")
    parser.add_argument("-z", "--zone", dest="zone_id", help="指定可用区 (配合 --price -t)")
    parser.add_argument("--list", action="store_true", help="列出所有可用选项 (grep 可筛)")
    return parser.parse_args()


# ======================================================================
# MAIN
# ======================================================================

def main():
    log("=" * 60)
    log("  ECS 抢占式实例迁移脚本")
    log(f"  区域: {REGION_ID}")
    log(f"  配置目录: {CONFIG_DIR}")
    log(f"  CSV 文件: {CSV_FILE}")
    log("=" * 60)

    # 校验目录是否存在
    if not CONFIG_DIR.exists():
        log("错误: config/ 目录不存在")
        sys.exit(1)

    client = AliClient()

    # ----- 断点恢复检测 ------------------------------------------------
    resume_result = handle_resume(client)
    if resume_result is not None:
        resume_run_id, csv_row = resume_result
    else:
        resume_run_id = None
        csv_row = None

    # ----- 确定 run_id ------------------------------------------------
    if resume_run_id:
        run_id = resume_run_id
    else:
        run_id = str(uuid.uuid4())[:8]

    # ----- 选择源实例 --------------------------------------------------
    if resume_run_id and csv_row:
        src_id = csv_row.get("source_instance_id", "").strip()
        if src_id:
            log(f"断点恢复: 使用源实例 {src_id}")
            source_inst = client.get_instance(src_id)
            if not source_inst:
                log(f"错误: 源实例 {src_id} 不存在了")
                sys.exit(1)
        else:
            source_inst = step_select_source(client)
            src_id = source_inst["InstanceId"]
    else:
        source_inst = step_select_source(client)
        src_id = source_inst["InstanceId"]
        if csv_row is None:
            # 新运行, 写 CSV 初始行
            csv_write_row(run_id, "", source_instance_id=src_id)

    source_name = source_inst.get("InstanceName", "")
    resource_name = generate_resource_name(source_name)
    log(f"资源命名: {resource_name}")
    source_disk_id = client.get_system_disk_id(src_id)
    if not source_disk_id:
        log("错误: 无法获取源实例系统盘 ID")
        sys.exit(1)

    # ----- 选择 YAML ---------------------------------------------------
    if resume_run_id and csv_row:
        yf = csv_row.get("yaml_config", "").strip()
        if yf and (CONFIG_DIR / yf).exists():
            log(f"断点恢复: 使用 YAML 配置 {yf}")
            with open(CONFIG_DIR / yf, "r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f) or {}
            yaml_fname = yf
        else:
            yaml_fname, yaml_data = step_select_yaml()
    else:
        yaml_fname, yaml_data = step_select_yaml()

    # 确定当前 checkpoint
    if csv_row:
        checkpoint = csv_row.get("checkpoint", "")
    else:
        checkpoint = ""

    # 记录 YAML 到 CSV
    csv_write_row(run_id, checkpoint, source_instance_id=src_id,
                  source_disk_id=source_disk_id, yaml_config=yaml_fname,
                  resource_name=resource_name)

    # ==================================================================
    # 执行阶段
    # ==================================================================

    log("\n即将开始创建云资源 (快照 → 镜像 → 新实例)。")
    if not confirm("开始执行?", default_yes=True):
        log("用户取消。断点已保留, 下次运行可恢复。")
        sys.exit(0)

    # --- 快照 ---
    if checkpoint in ("", CK_SNAPSHOT_CREATING, CK_SNAPSHOT_DONE, CK_IMAGE_CREATING,
                       CK_IMAGE_DONE, CK_INSTANCE_CREATING, CK_INSTANCE_DONE, CK_DONE):
        csv_write_row(run_id, CK_SNAPSHOT_CREATING, source_instance_id=src_id,
                      source_disk_id=source_disk_id, yaml_config=yaml_fname,
                      resource_name=resource_name)
        snapshot_id = step_create_snapshot(client, source_disk_id, resource_name, csv_row or {})
        if snapshot_id:
            csv_write_row(run_id, CK_SNAPSHOT_CREATING, snapshot_id=snapshot_id,
                          source_instance_id=src_id, source_disk_id=source_disk_id,
                          yaml_config=yaml_fname, resource_name=resource_name)
            log("  等待快照完成 (请勿中断)...")
            if not client.poll_snapshot(snapshot_id):
                log("快照创建未成功, 退出。断点已保存。")
                sys.exit(1)
            csv_write_row(run_id, CK_SNAPSHOT_DONE, snapshot_id=snapshot_id,
                          source_instance_id=src_id, source_disk_id=source_disk_id,
                          yaml_config=yaml_fname, resource_name=resource_name)
            log(f"  快照完成: {snapshot_id}")
        else:
            log("快照创建失败, 退出。")
            sys.exit(1)
    else:
        snapshot_id = csv_row.get("snapshot_id", "") if csv_row else ""

    # --- 镜像 ---
    if checkpoint in ("", CK_SNAPSHOT_DONE, CK_IMAGE_CREATING, CK_IMAGE_DONE,
                       CK_INSTANCE_CREATING, CK_INSTANCE_DONE, CK_DONE):
        csv_write_row(run_id, CK_IMAGE_CREATING, snapshot_id=snapshot_id,
                      source_instance_id=src_id, source_disk_id=source_disk_id,
                      yaml_config=yaml_fname, resource_name=resource_name)
        image_id = step_create_image(client, snapshot_id, resource_name, csv_row or {})
        if image_id:
            csv_write_row(run_id, CK_IMAGE_CREATING, snapshot_id=snapshot_id,
                          image_id=image_id, source_instance_id=src_id,
                          source_disk_id=source_disk_id, yaml_config=yaml_fname,
                           resource_name=resource_name)
            log("  等待镜像完成 (请勿中断)...")
            if not client.poll_image(image_id):
                log("镜像创建未成功, 退出。断点已保存。")
                sys.exit(1)
            csv_write_row(run_id, CK_IMAGE_DONE, snapshot_id=snapshot_id,
                          image_id=image_id, source_instance_id=src_id,
                          source_disk_id=source_disk_id, yaml_config=yaml_fname,
                          resource_name=resource_name)
            log(f"  镜像完成: {image_id}")
        else:
            log("镜像创建失败, 退出。")
            sys.exit(1)
    else:
        image_id = csv_row.get("image_id", "") if csv_row else ""

    # --- 合并配置 + 确认 ---
    merged_config = step_merge_and_confirm(
        client, source_inst, yaml_fname, yaml_data, resource_name, image_id)

    # --- 查询抢占式报价 ---
    inst_type = merged_config.get("InstanceType", "")
    zone_id = merged_config.get("ZoneId", "")
    if inst_type and zone_id:
        log(f"\n  查询 {inst_type} 在 {zone_id} 的抢占式报价...")
        spot_price_data = client.query_spot_price(inst_type, zone_id)
        if spot_price_data:
            spot_price = spot_price_data.get("SpotPrice", 0)
            origin_price = spot_price_data.get("OriginPrice", 0)
            discount = (1 - spot_price / (origin_price or 1)) * 100
            monthly_spot = spot_price * 24 * 30
            monthly_origin = origin_price * 24 * 30
            print(f"\n  当前抢占式报价:")
            print(f"    规格:     {inst_type}")
            print(f"    可用区:   {zone_id}")
            print(f"    原价:     {origin_price:.3f} 元/小时  (≈{monthly_origin:.1f} 元/月)")
            print(f"    抢占价:   {spot_price:.3f} 元/小时  (≈{monthly_spot:.1f} 元/月)")
            print(f"    折扣:     {discount:.0f}% off")
        else:
            log("    未获取到报价数据 (可能该规格不支持抢占式)")

    if not confirm("\n以上配置确认无误, 继续创建新实例?"):
        log("用户取消。断点已保留, 可恢复。")
        sys.exit(0)

    # --- 创建新实例 ---
    if checkpoint in ("", CK_IMAGE_DONE, CK_INSTANCE_CREATING, CK_INSTANCE_DONE, CK_DONE):
        csv_write_row(run_id, CK_INSTANCE_CREATING, snapshot_id=snapshot_id,
                      image_id=image_id, source_instance_id=src_id,
                      source_disk_id=source_disk_id, yaml_config=yaml_fname,
                      resource_name=resource_name)
        new_instance_id = step_create_instance(client, merged_config, csv_row or {})
        if new_instance_id:
            csv_write_row(run_id, CK_INSTANCE_CREATING, snapshot_id=snapshot_id,
                          image_id=image_id, source_instance_id=src_id,
                          source_disk_id=source_disk_id, yaml_config=yaml_fname,
                           new_instance_id=new_instance_id, resource_name=resource_name)
            log("  等待实例启动 (请勿中断)...")
            if not client.poll_instance_running(new_instance_id):
                log("实例未成功启动, 退出。断点已保存。")
                sys.exit(1)
            csv_write_row(run_id, CK_INSTANCE_DONE, snapshot_id=snapshot_id,
                          image_id=image_id, source_instance_id=src_id,
                          source_disk_id=source_disk_id, yaml_config=yaml_fname,
                          new_instance_id=new_instance_id, resource_name=resource_name)
            log(f"  新实例已 Running: {new_instance_id}")
        else:
            log("实例创建失败, 退出。")
            sys.exit(1)

    # --- 清理旧资源 ---
    step_cleanup(client, src_id, snapshot_id, image_id, run_id)

    # --- DONE ---
    csv_write_row(run_id, CK_DONE, snapshot_id=snapshot_id, image_id=image_id,
                  source_instance_id=src_id, source_disk_id=source_disk_id,
                  yaml_config=yaml_fname, new_instance_id=new_instance_id,
                  resource_name=resource_name)

    print_box("完成", [
        f"源实例 {src_id} 已释放",
        f"新实例 {new_instance_id} 已运行",
        f"快照 {snapshot_id} 已保留",
        f"镜像 {image_id} 已保留",
        f"CSV 已更新: {CSV_FILE}",
    ])


if __name__ == "__main__":
    try:
        args = parse_args()

        client = AliClient()

        if args.list:
            cmd_list(client)
            sys.exit(0)

        if args.price:
            if args.instance_type:
                cmd_price_specific(client, args.instance_type, args.zone_id)
            else:
                cmd_price(client)
            sys.exit(0)

        main()
    except KeyboardInterrupt:
        print("\n用户中断 (Ctrl+C)。断点已保存, 下次运行可恢复。")
        sys.exit(0)
