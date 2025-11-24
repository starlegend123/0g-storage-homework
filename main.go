package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// 配置区域
const (
	// Client 路径 (Windows 记得带 .exe)
	ClientBinaryPath = "./0g-storage-client.exe"

	// 0G Testnet RPC
	JsonRpc = "https://evm-rpc.0g.ai"

	// [关键修改] 使用 Indexer 地址，而不是 StorageNode
	IndexerUrl = "https://indexer-storage-testnet.0g.ai"

	// 你的私钥 (去掉 0x)
	PrivateKey = ""

	TestFileName = "test_4gb_data.bin"
	FileSize     = 4 * 1024 * 1024 * 1024 // 4GB
	FragmentSize = 400 * 1024 * 1024      // 400MB
)

func main() {
	// 1. 生成 4GB 测试文件
	fmt.Println(">>> 步骤 1: 生成 4GB 测试文件...")
	err := generateLargeFile(TestFileName, FileSize)
	if err != nil {
		fmt.Printf("生成文件失败: %v\n", err)
		return
	}
	// 程序结束后删除测试文件(可选)
	// defer os.Remove(TestFileName)
	fmt.Printf("文件 %s 生成完毕，大小: %d bytes\n", TestFileName, FileSize)

	// 2. 执行上传命令
	fmt.Println("\n>>> 步骤 2: 使用 0g-storage-client 上传 (设置 fragment-size)...")
	fmt.Println("正在连接 Indexer 寻找可用节点，请耐心等待...")

	uploadArgs := []string{
		"upload",
		"--url", JsonRpc,
		"--key", PrivateKey,
		"--file", TestFileName,
		"--fragment-size", fmt.Sprintf("%d", FragmentSize),
		// [关键修改] 这里使用的是 --indexer 和 IndexerUrl
		"--indexer", IndexerUrl,
	}

	output, err := runCommand(ClientBinaryPath, uploadArgs...)
	if err != nil {
		fmt.Printf("上传失败: %v\n", err)
		return
	}

	// 3. 从输出中解析 Root Hash
	rootHash := parseRootHash(output)
	if rootHash == "" {
		fmt.Println("错误: 无法从输出中获取 Root Hash，请检查上方日志")
		return
	}
	fmt.Printf(">>> 上传成功! Root Hash: %s\n", rootHash)

	// 4. 执行下载命令
	fmt.Println("\n>>> 步骤 3: 下载文件进行验证...")
	downloadArgs := []string{
		"download",
		"--root", rootHash,
		"--url", JsonRpc,
		"--indexer", IndexerUrl, // 下载也加上 indexer 更保险
		"--output", "downloaded_" + TestFileName,
	}

	_, err = runCommand(ClientBinaryPath, downloadArgs...)
	if err != nil {
		fmt.Printf("下载失败: %v\n", err)
		return
	}
	fmt.Printf(">>> 文件下载成功: downloaded_%s\n", TestFileName)
}

// --- 下面是辅助函数，保持不变 ---

func generateLargeFile(filename string, size int64) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Seek(size-1, 0)
	if err != nil {
		return err
	}
	_, err = f.Write([]byte{0})
	return err
}

func runCommand(command string, args ...string) (string, error) {
	// 打印一下具体执行了什么命令，方便调试
	fmt.Printf("执行命令: %s %s\n", command, strings.Join(args, " "))

	cmd := exec.Command(command, args...)
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return "", err
	}

	var fullOutput strings.Builder
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		m := scanner.Text()
		fmt.Println(m) // 实时打印日志
		fullOutput.WriteString(m + "\n")
	}

	if err := cmd.Wait(); err != nil {
		return fullOutput.String(), err
	}
	return fullOutput.String(), nil
}

func parseRootHash(log string) string {
	re := regexp.MustCompile(`root[:\s]+(0x[a-fA-F0-9]{64})`)
	matches := re.FindStringSubmatch(log)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}
