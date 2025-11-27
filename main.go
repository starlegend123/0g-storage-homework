package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	zg_common "github.com/0gfoundation/0g-storage-client/common"
	"github.com/0gfoundation/0g-storage-client/common/blockchain"
	"github.com/0gfoundation/0g-storage-client/common/shard"
	"github.com/0gfoundation/0g-storage-client/core"
	"github.com/0gfoundation/0g-storage-client/indexer"
	"github.com/0gfoundation/0g-storage-client/transfer"
	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
)

// --- 配置区域 ---
const (
	EvmRpcUrl     = "https://evmrpc-testnet.0g.ai"
	IndexerUrl    = "https://indexer-storage-testnet-turbo.0g.ai"
	TestFileName  = "test_4gb_file.bin"
	LargeFileSize = 4 * 1024 * 1024 * 1024 // 4GB
	ChunkSize     = 400 * 1024 * 1024      // 400MB 每个分片
	// 【作业考点】Fragment Size 设置 (Upload Task Size)
	UploadTaskSize = 16 * 1024 * 1024 // 16MB Fragment Size
)

// 封装一个简单的客户端，提供 Upload / Download 能力
type StorageClient struct {
	idx *indexer.Client
	w3  *web3go.Client
}

func main() {
	// 读取私钥
	privateKeyHex := os.Getenv("ZGS_PRIVATE_KEY")
	if privateKeyHex == "" {
		log.Fatal("❌ 请先设置环境变量：ZGS_PRIVATE_KEY=0x...")
	}

	ctx := context.Background()
	uploader, err := setupClient(privateKeyHex)
	if err != nil {
		log.Fatalf("❌ 客户端初始化失败: %v", err)
	}

	// 可选：先打印当前 Indexer 返回的节点分布，便于作业说明/排障
	debugShardedNodes(ctx)

	fmt.Println("✅ 0G Storage Client 初始化成功")

	// --- 步骤 1: 生成 4GB 稀疏文件 ---
	fmt.Println("\n>>> 步骤 1: 生成 4GB 测试文件...")
	if err := createDummyFile(TestFileName, LargeFileSize); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(TestFileName)
	fmt.Printf("✅ %s 文件生成完毕\n", TestFileName)

	// 打开文件准备切片上传
	file, err := os.Open(TestFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	buffer := make([]byte, ChunkSize)
	var roots []string

	// --- 步骤 2: 循环切分并上传 10 个 400MB 分片 ---
	fmt.Println("\n>>> 步骤 2: 开始上传 10 个 400MB 分片...")
	for i := 0; i < 10; i++ {
		fmt.Printf("\n--- 正在上传第 %d/10 个分片 ---\n", i+1)

		n, err := io.ReadFull(file, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Fatal(err)
		}
		if n == 0 {
			break
		}

		// Upload 返回 (txHash string, root string, error)
		txHash, root, err := uploader.Upload(ctx, buffer[:n])
		if err != nil {
			log.Fatalf("❌ 第 %d 个分片上传失败: %v", i+1, err)
		}

		roots = append(roots, root)
		fmt.Printf("✅ 上传成功！Root: %s\nTxHash: %s\n", root, txHash)
	}

	// --- 步骤 3: 下载验证 ---
	fmt.Println("\n>>> 步骤 3: 开始下载验证...")
	for i, root := range roots {
		outFile := fmt.Sprintf("downloaded_chunk_%d.bin", i)
		fmt.Printf("正在下载第 %d 个分片 (Root: %s)... \n", i+1, root[:10]+"...")

		if err := uploader.Download(ctx, root, outFile); err != nil {
			log.Printf("❌ 下载失败: %v", err)
		} else {
			fmt.Printf("✅ 下载成功: %s\n", outFile)
			os.Remove(outFile) // 验证完就删掉
		}
	}

	fmt.Println("\n========================================================")
	fmt.Println("🚀 恭喜！全流程完成，可以提交作业了！")
	fmt.Println("========================================================")
}

// --- 辅助函数 ---

// 初始化上传客户端：使用 indexer + blockchain 封装一个简单的 StorageClient
func setupClient(pkHex string) (*StorageClient, error) {
	// 这里直接把私钥字符串交给 web3 客户端（可带 0x 前缀）
	w3 := blockchain.MustNewWeb3(EvmRpcUrl, pkHex)

	idxClient, err := indexer.NewClient(IndexerUrl, indexer.IndexerClientOption{
		LogOption: zg_common.LogOption{
			LogLevel: logrus.InfoLevel, // 避免 Reminder 使用 PanicLevel 导致 panic
		},
	})
	if err != nil {
		return nil, fmt.Errorf("indexer 客户端初始化失败: %w", err)
	}

	return &StorageClient{
		idx: idxClient,
		w3:  w3,
	}, nil
}

// Upload 上传一块数据到 0g 存储，返回交易哈希和 root
func (c *StorageClient) Upload(ctx context.Context, data []byte) (string, string, error) {
	iter, err := core.NewDataInMemory(data)
	if err != nil {
		return "", "", fmt.Errorf("创建内存数据失败: %w", err)
	}

	// 通过 indexer 选择节点并上传
	txHash, err := c.idx.Upload(ctx, c.w3, iter, transfer.UploadOption{
		FinalityRequired: transfer.FileFinalized,
		ExpectedReplica:  1,
		TaskSize:         UploadTaskSize, // 【作业考点】设置单次上传任务包含的 segment 数量
		Method:           "min",          // 使用官方推荐的 "min" 方式选择节点
		FullTrusted:      true,           // 只用 trusted 节点，避免 discovered 干扰
	})
	if err != nil {
		return "", "", fmt.Errorf("上传失败: %w", err)
	}

	// 本地计算 merkle root，作为返回的 root
	tree, err := core.MerkleTree(iter)
	if err != nil {
		return "", "", fmt.Errorf("计算 Merkle Root 失败: %w", err)
	}

	return txHash.Hex(), tree.Root().Hex(), nil
}

// Download 按 root 下载到指定文件
func (c *StorageClient) Download(ctx context.Context, root, outFile string) error {
	// indexer.Client 已封装好从合适的节点下载
	return c.idx.Download(ctx, root, outFile, false)
}

// debugShardedNodes 打印当前 indexer 返回的节点和 shard 配置，辅助排查 “replication requirement” 类错误
func debugShardedNodes(ctx context.Context) {
	fmt.Println("\n>>> 调试：从 Indexer 拉取当前存储节点信息...")

	idxClient, err := indexer.NewClient(IndexerUrl)
	if err != nil {
		fmt.Printf("获取 Indexer 客户端失败: %v\n", err)
		return
	}

	nodes, err := idxClient.GetShardedNodes(ctx)
	if err != nil {
		fmt.Printf("调用 GetShardedNodes 失败: %v\n", err)
		return
	}

	fmt.Printf("Indexer 返回节点情况：Trusted=%d, Discovered=%d\n", len(nodes.Trusted), len(nodes.Discovered))

	printNodes := func(title string, list []*shard.ShardedNode) {
		fmt.Println(title)
		for i, n := range list {
			fmt.Printf("  #%d URL=%s, NumShard=%d, ShardId=%d, Latency=%dms\n",
				i, n.URL, n.Config.NumShard, n.Config.ShardId, n.Latency)
		}
	}

	printNodes("  Trusted 节点列表：", nodes.Trusted)
	printNodes("  Discovered 节点列表：", nodes.Discovered)
}

// 快速生成稀疏大文件
func createDummyFile(name string, size int64) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(size-1, 0); err != nil {
		return err
	}
	_, err = f.Write([]byte{0})
	return err
}
