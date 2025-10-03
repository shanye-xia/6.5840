package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// ByKey 用于按 key 对中间结果排序
type ByKey []mr.KeyValue

// 实现 sort.Interface 接口
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
    if len(os.Args) < 3 {
        fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
        os.Exit(1)
    }

    // 动态加载 Map 和 Reduce 函数
    mapf, reducef := loadPlugin(os.Args[1])

    //
    // 读取每个输入文件，调用 Map，收集所有中间结果
    //
    intermediate := []mr.KeyValue{}
    for _, filename := range os.Args[2:] {
        file, err := os.Open(filename)
        if err != nil {
            log.Fatalf("cannot open %v", filename)
        }
        content, err := ioutil.ReadAll(file)
        if err != nil {
            log.Fatalf("cannot read %v", filename)
        }
        file.Close()
        // 调用 Map 函数，处理文件内容
        kva := mapf(filename, string(content))
        // 累加所有中间 key-value 对
        intermediate = append(intermediate, kva...)
    }

    //
    // 与真实分布式 MapReduce 最大的不同：所有中间数据都在一个切片 intermediate[] 里，
    // 而不是分成 NxM 个桶（分布式环境下会分桶以便并行处理）。
    //

    // 按 key 对中间结果排序，方便后续 Reduce 聚合
    sort.Sort(ByKey(intermediate))

    oname := "mr-out-0"
    ofile, _ := os.Create(oname)

    //
    // 对每个不同的 key 调用 Reduce，并将结果输出到 mr-out-0 文件
    //
    i := 0
    for i < len(intermediate) {
        j := i + 1
        // 找到所有 key 相同的区间 [i, j)
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        values := []string{}
        for k := i; k < j; k++ {
            values = append(values, intermediate[k].Value)
        }
        // 调用 Reduce 函数，处理聚合后的 values
        output := reducef(intermediate[i].Key, values)

        // 以 "key value" 格式写入输出文件
        fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

        i = j
    }

    ofile.Close()
}

// 动态加载插件文件中的 Map 和 Reduce 函数
// 返回两个函数指针，供主流程调用
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
    p, err := plugin.Open(filename)
    if err != nil {
        log.Fatalf("cannot load plugin %v", filename)
    }
    xmapf, err := p.Lookup("Map")
    if err != nil {
        log.Fatalf("cannot find Map in %v", filename)
    }
    mapf := xmapf.(func(string, string) []mr.KeyValue)
    xreducef, err := p.Lookup("Reduce")
    if err != nil {
        log.Fatalf("cannot find Reduce in %v", filename)
    }
    reducef := xreducef.(func(string, []string) string)

    return mapf, reducef
}
