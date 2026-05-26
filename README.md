# ECS 抢占式实例迁移脚本

## 功能

将阿里云深圳区 (cn-shenzhen) 抢占式 ECS 实例迁移到新规格：
快照系统盘 → 创建镜像 → 新建抢占式实例 → 释放老实例 + 清理历史快照/镜像。

支持断点续传，中途被杀可恢复继续。

## 环境准备

```bash
pip install aliyun-python-sdk-ecs pyyaml python-dotenv

# 复制环境变量文件
cp .env.example .env
# 编辑 .env，填入真实 AK
vim .env
```

AccessKey 获取：RAM 控制台 → 用户 → 创建 AccessKey。用 RAM 用户，别用主账号。

## YAML 配置

在 `config/` 目录下放 `.yaml` 文件。指定字段覆盖源实例，未指定字段自动继承。

```yaml
# config/small_4u8g.yaml
name: "小型 4U8G"
instance_type: ecs.sn1ne.xlarge
spot_strategy: SpotAsPriceGo
spot_duration: 0
spot_interruption_behavior: Stop
internet_max_bandwidth_out: 100
internet_charge_type: PayByTraffic
disk_size: 35
```

关键字段说明：
- `instance_type` — 实例规格
- `spot_duration` — 0 = 无保护期，1 = 保护 1 小时
- `spot_interruption_behavior` — `Stop` = 中断时节省停机 (省钱)
- `internet_max_bandwidth_out` — 公网出带宽上限 (Mbps)
- `internet_charge_type` — `PayByTraffic` (按流量) 或 `PayByBandwidth` (按带宽)
- `disk_size` — 系统盘大小 (GiB)，无数据盘

任意字段可增删，缺失字段自动从源实例继承。

## 运行

```bash
# 正常迁移流程
python3 migrate_spot_instance.py

# 列出抢占式实例 + YAML 配置 + CSV 历史 (grep 友好)
python3 migrate_spot_instance.py --list

# 查询所有 YAML 配置规格的抢占式报价
python3 migrate_spot_instance.py --price

# 查询指定规格 + 可用区的抢占式报价
python3 migrate_spot_instance.py --price -t ecs.c6e.13xlarge -z cn-shenzhen-f
```

## 执行流程

1. 列出深圳区抢占式实例，选择一台 (按 `q` 退出)
2. 选择 YAML 配置 (按 `q` 退出)
3. 确认"开始执行？" — 即将创建快照/镜像/实例，此时退出无任何资源产生
4. 创建源实例系统盘快照，轮询等待 (请勿中断)
5. 从快照创建自定义镜像，轮询等待 (请勿中断)
6. 展示合并后的实例配置 + 当前抢占式报价，确认后创建实例
7. 创建新抢占式实例，轮询等待启动 (请勿中断)
8. 清理：释放老实例 + 删除 CSV 历史中的旧快照/镜像 (请勿中断)
9. 完成，打印摘要

## 资源命名

格式: `<基础名>.<YY.MM.DD.HH.mm>` (UTC+8 时间)

基础名 = 源实例名去除末尾时间戳。脚本识别并去除两种时间戳格式：

| 源实例名                  | 去除后       | 新名称                  |
|--------------------------|-------------|------------------------|
| `mintp.26.05.25`         | `mintp`     | `mintp.26.05.26.15.32` |
| `mintp.26.05.25.12.48`  | `mintp`     | `mintp.26.05.26.15.32` |
| `my-server` (无时间戳)    | `my-server` | `my-server.26.05.26.15.32` |

快照、镜像、新实例三者共用同一名称，在脚本启动时生成。

## 中断与退出

**可安全退出** (按 `q` 或 Ctrl+C)：
- 选择源实例 / YAML 配置 (步骤 1-2)
- "开始执行？"确认提示 (步骤 3)
- 实例配置确认提示 (步骤 6)

**不要中断** (资源可能停留在半创建状态)：
- 快照/镜像/实例轮询等待 (步骤 4, 5, 7)
- 清理老资源 (步骤 8)

轮询期间 Ctrl+C：脚本捕获，保存断点后退出，下次可恢复继续。

## 断点续传

CSV 文件 (`resources.csv`) 记录执行进度。字段：
`run_id, checkpoint, snapshot_id, image_id, source_instance_id,
 source_disk_id, new_instance_id, yaml_config, resource_name, timestamp`

脚本被中断后：
- 重新运行脚本
- 自动检测未完成的任务 (checkpoint != `done`)
- 验证云端状态与断点一致
- 询问：[R] 从断点恢复，[S] 重新开始

断点阶段: `snapshot_creating` → `snapshot_done` → `image_creating` → `image_done` → `instance_creating` → `instance_done` → `done`

## 清理

新实例启动后：
- 释放老源实例
- 删除 CSV 中历史运行的快照和镜像 (不删本次运行的)
- CSV 保留当前运行的行

## 注意事项

- 新实例使用镜像预设密码 (PasswordInherit=true)
- 安全组、交换机、可用区、密钥对、RAM 角色、资源组全部从源实例继承
- 竞价策略：SpotAsPriceGo + Stop (节省停机) + spot_duration=0
- 无数据盘，系统盘大小从 YAML 读取 (默认 35G)
- 节省停机实例在 DescribeInstances 中 SystemDisk 为空，脚本自动走 DescribeDisks 回退查询
- CSV 中的旧快照/镜像若已不存在，删除时自动跳过 (NotFound)
- 被实例占用的镜像无法普通删除，使用 force 模式删除
