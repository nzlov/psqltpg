package main

import (
	"flag"
	"fmt"
	"time"

	"strconv"
)

var (
	fd  = flag.String("d", "1y", "周期(y,m,d)：\n \"1\" 1年\n \"2m\" 2个月\n")
	ff  = flag.String("f", "", "创建新表附加字段列表")
	fp  = flag.Bool("p", false, "跳过当前周期")
	fs  = flag.Int64("s", time.Now().Unix(), "开始时间")
	fc  = flag.Int("c", 0, "创建主分区(0,1,2,3,4):\n 0 不创建\n 1 创建\n 2 重名创建\n 3 迁移老数据\n 4 迁移并清理老数据\n")
	fdd = flag.Int("dd", 0, "default分区(0,1,2,3,4):\n 0 不禁用\n 1 创建\n 2 禁用\n 3 禁用并转移default\n 4 禁用并转移清理default\n")

	l = "y"
)

func main() {
	flag.Parse()
	if len(flag.Args()) == 0 {
		flag.PrintDefaults()
		return
	}

	ranges := genRange()

	for i, t := range flag.Args() {
		if i != 0 {
			fmt.Println("========================================================================================")
		}
		if *fc > 0 {
			if *fc > 1 {
				fmt.Printf("ALTER TABLE %s RENAME TO %s_old;\n", t, t)
			}
			if *ff == "" {
				fmt.Printf("CREATE TABLE %s (id serial,addtime int8) PARTITION BY RANGE(addtime);\n", t)
			} else {
				fmt.Printf("CREATE TABLE %s (id serial,addtime int8,%s) PARTITION BY RANGE(addtime);\n", t, *ff)
			}
		}
		if *fdd > 1 {
			fmt.Printf("ALTER TABLE %s DETACH PARTITION %s_default;\n", t, t)
		}

		for i := 0; i < len(ranges)-1; i++ {
			fmt.Printf("CREATE TABLE %s_%st%s PARTITION OF %s FOR VALUES FROM (%v) TO (%v);\n", t, getTableS(ranges[i]), getTableS(ranges[i+1]), t, ranges[i].Unix(), ranges[i+1].Unix())
		}

		if *fdd > 1 {
			if *fdd > 2 {
				fmt.Printf("INSERT INTO %s SELECT * FROM %s_default;\n", t, t)
				if *fdd == 4 {
					fmt.Printf("TRUNCATE TABLE %s_default;\n", t)
				}
			}
			fmt.Printf("ALTER TABLE %s ATTACH PARTITION %s_default DEFAULT;\n", t, t)
		} else if *fdd == 1 {
			fmt.Printf("CREATE TABLE %s_default PARTITION OF %s DEFAULT;\n", t, t)
		}

		if *fc > 2 {
			fmt.Printf("INSERT INTO %s SELECT * FROM %s_old;\n", t, t)
			if *fc == 4 {
				fmt.Printf("TRUNCATE TABLE %s_old;\n", t)
			}
		}
	}
}

func getTableS(t time.Time) string {
	switch l {
	case "m":
		return t.Format("2006_01")
	case "d":
		return t.Format("2006_01_02")
	default:
		return t.Format("2006")
	}
}

func genRange() []time.Time {
	y, m, d := 0, 0, 0
	dl := getd()
	if dl < 1 {
		panic("间隔错误")
	}
	switch l {
	case "m":
		m += dl
	case "d":
		d += dl
	default:
		y += dl

	}

	start := gets()
	if *fp {
		start = start.AddDate(y, m, d)
	}
	r := []time.Time{start}

	for i := 0; i < dl; i++ {
		start = start.AddDate(y, m, d)
		r = append(r, start)
	}
	return r
}

func gets() time.Time {
	now := time.Unix(*fs, 0)
	y, m, d := now.Date()
	switch l {
	case "y":
		m = 1
		d = 1
	case "m":
		d = 1
	}
	st := time.Date(y, m, d, 0, 0, 0, 0, now.Location())

	return st
}

func getd() int {
	ls := string((*fd)[len(*fd)-1])
	n := int64(0)
	l = "y"
	var err error
	switch ls {
	case "y", "m", "d":
		n, err = strconv.ParseInt(string((*fd)[:len((*fd))-1]), 10, 64)
		l = ls
	case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9":
		n, err = strconv.ParseInt(*fd, 10, 64)
	default:
		panic("间隔类型错误")
	}
	if err != nil {
		panic("间隔错误")
	}
	return int(n)

}
