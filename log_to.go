package judb

import (
	"fmt"
	"github.com/jsuserapp/ju"
	"strings"
	"sync"
)

var _logMutex sync.Mutex

func logToColor(skip int, c, tab string, v ...interface{}) {
	trace := ju.GetTrace(skip)

	var builder strings.Builder
	for i, value := range v {
		if i > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString(fmt.Sprint(value))
	}
	str := builder.String()
	saveLog(logParam.DbType, tab, trace, c, str)
	cp := ju.GetColorPrint(c)

	_logMutex.Lock()
	defer _logMutex.Unlock()
	fmt.Print(ju.GetNowTimeMs(), " ", trace, " ")
	cp("%s\n", str)
}

func logToColorF(skip int, c, tab, format string, v ...interface{}) {
	trace := ju.GetTrace(skip)
	saveLog(logParam.DbType, tab, trace, c, fmt.Sprintf(format, v...))
	cp := ju.GetColorPrint(c)

	_logMutex.Lock()
	defer _logMutex.Unlock()
	fmt.Print(ju.GetNowTimeMs(), " ", trace, " ")
	cp(format, v...)
}

// noinspection GoUnusedExportedFunction
func LogToBlack(tab string, a ...interface{}) { logToColor(3, "black", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToRed(tab string, a ...interface{}) { logToColor(3, "red", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToGreen(tab string, a ...interface{}) { logToColor(3, "green", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToYellow(tab string, a ...interface{}) { logToColor(3, "yellow", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToBlue(tab string, a ...interface{}) { logToColor(3, "blue", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToMagenta(tab string, a ...interface{}) { logToColor(3, "magenta", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToCyan(tab string, a ...interface{}) { logToColor(3, "cyan", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToWhite(tab string, a ...interface{}) { logToColor(3, "white", tab, a...) }

// noinspection GoUnusedExportedFunction
func LogToBlackF(tab, format string, a ...interface{}) { logToColorF(3, "black", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToRedF(tab, format string, a ...interface{}) { logToColorF(3, "red", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToGreenF(tab, format string, a ...interface{}) { logToColorF(3, "green", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToYellowF(tab, format string, a ...interface{}) { logToColorF(3, "yellow", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToBlueF(tab, format string, a ...interface{}) { logToColorF(3, "blue", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToMagentaF(tab, format string, a ...interface{}) {
	logToColorF(3, "magenta", tab, format, a...)
}

// noinspection GoUnusedExportedFunction
func LogToCyanF(tab, format string, a ...interface{}) { logToColorF(3, "cyan", tab, format, a...) }

// noinspection GoUnusedExportedFunction
func LogToWhiteF(tab, format string, a ...interface{}) { logToColorF(3, "white", tab, format, a...) }
