package console

/*
	This file contains functionality related to rendering templates
*/

import (
	"html/template"
	"net/http"
	"time"

	"encoding/base32"

	"code.google.com/p/log4go"
	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/unrolled/render"
)

var zeroTime = time.Time{}
var zeroUuid = gocql.UUID{}
var timeFormat = "2006-01-02 15:04:05 -0700"

const PageWindowLength = 15

func yesOnFilledFunc(s string) string {
	if s == "" {
		return ""
	} else {
		return "yes"
	}
}

func yesOnTrueFunc(q bool) string {
	if q {
		return "yes"
	} else {
		return ""
	}

}

func activeSinceFunc(t time.Time) string {
	if t == zeroTime {
		return ""
	} else {
		return t.Format(timeFormat)
	}
}

func ftimeFunc(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return "Not yet crawled"
	} else {
		return t.Format(timeFormat)
	}
}

func ftime2Func(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return ""
	} else {
		return t.Format(timeFormat)
	}
}

func fuuidFunc(u gocql.UUID) string {
	if u == zeroUuid {
		return ""
	} else {
		return u.String()
	}
}

var Render *render.Render

func BuildRender() {
	Render = render.New(render.Options{
		Directory:     walker.Config.Console.TemplateDirectory,
		Layout:        "layout",
		IndentJSON:    true,
		IsDevelopment: true,
		Funcs: []template.FuncMap{
			template.FuncMap{
				"yesOnFilled": yesOnFilledFunc,
				"activeSince": activeSinceFunc,
				"ftime":       ftimeFunc,
				"ftime2":      ftime2Func,
				"fuuid":       fuuidFunc,
				"statusText":  http.StatusText,
				"yesOnTrue":   yesOnTrueFunc,
			},
		},
	})
}

func replyServerError(w http.ResponseWriter, err error) {
	log4go.Error("Rendering 500: %v", err)
	mp := map[string]interface{}{
		"anErrorHappend": true,
		"theError":       err.Error(),
	}
	Render.HTML(w, http.StatusInternalServerError, "serverError", mp)
	return
}

// Some Utilities
func decode32(s string) (string, error) {
	b, err := base32.StdEncoding.DecodeString(s)
	return string(b), err
}

func encode32(s string) string {
	b := base32.StdEncoding.EncodeToString([]byte(s))
	return string(b)
}
