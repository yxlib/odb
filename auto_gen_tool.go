// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gamedb

import (
	"os"

	"github.com/yxlib/yx"
)

func GenRowObjRegisterFileByCfg(cfg *Config, regFilePath string, regPackName string) {
	f, err := os.OpenFile(regFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return
	}

	defer f.Close()

	writePackage(f, regPackName)

	rowObjClasses := yx.NewSet(yx.SET_TYPE_OBJ)
	for _, storageCfg := range cfg.Storages {
		for _, workerCfg := range storageCfg.Workers {
			rowObjClasses.Add(workerCfg.RowObj)
		}
	}
	writeImport(rowObjClasses, regPackName, f)
	writeRegFunc(rowObjClasses, regPackName, f)
}

func writePackage(f *os.File, regPackName string) {
	f.WriteString("// This File auto generate by tool.\n")
	f.WriteString("// Please do not modify.\n")
	f.WriteString("// See gamedb.GenRowObjRegisterFileByCfg().\n\n")
	f.WriteString("package " + regPackName + "\n\n")
}

func writeImport(rowObjClasses *yx.Set, regPackName string, f *os.File) {
	f.WriteString("import (\n")

	packSet := yx.NewSet(yx.SET_TYPE_OBJ)
	packSet.Add("github.com/yxlib/gamedb")

	classNames := rowObjClasses.GetElements()
	for _, className := range classNames {
		addRowClassPackage(className.(string), regPackName, packSet)
	}

	elements := packSet.GetElements()
	for _, packName := range elements {
		f.WriteString("    \"" + packName.(string) + "\"\n")
	}

	f.WriteString(")\n\n")
}

func addRowClassPackage(className string, regPackName string, packSet *yx.Set) {
	if className == "" {
		return
	}

	fullPackName := yx.GetFullPackageName(className)
	filePackName := yx.GetFilePackageName(fullPackName)
	if filePackName != regPackName {
		packSet.Add(fullPackName)
	}
}

func writeRegFunc(rowObjClasses *yx.Set, regPackName string, f *os.File) {
	f.WriteString("// Auto generate by tool.\n")
	f.WriteString("func RegisterRowObjs() {\n")

	classNames := rowObjClasses.GetElements()
	for _, classNameObj := range classNames {
		classReflectName, _ := classNameObj.(string)
		f.WriteString("    // " + classReflectName + "\n")

		if classReflectName == "" {
			println("[Error]    class name can not be empty!!!")
			break
		}

		fullPackName := yx.GetFullPackageName(classReflectName)
		filePackName := yx.GetFilePackageName(fullPackName)
		filePackClassName := yx.GetFilePackageClassName(classReflectName)
		if filePackName == regPackName {
			filePackClassName = yx.GetClassName(classReflectName)
		}
		f.WriteString("    gamedb.RowObjRegister.RegisterObject(&" + filePackClassName + "{}, nil, 10)\n")

		f.WriteString("\n")
	}

	f.WriteString("}")
}
