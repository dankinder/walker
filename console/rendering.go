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
	"github.com/gorilla/sessions"
	"github.com/iParadigms/walker"
	"github.com/unrolled/render"
)

var zeroTime = time.Time{}
var zeroUUID = gocql.UUID{}
var timeFormat = "2006-01-02 15:04:05 -0700"

func yesOnFilledFunc(s string) string {
	if s == "" {
		return ""
	}
	return "yes"

}

func yesOnTrueFunc(q bool) string {
	if q {
		return "yes"
	}
	return ""
}

func activeSinceFunc(t time.Time) string {
	if t == zeroTime {
		return ""
	}
	return t.Format(timeFormat)
}

func ftimeFunc(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return "Not yet crawled"
	}
	return t.Format(timeFormat)
}

func ftime2Func(t time.Time) string {
	if t == zeroTime || t.Equal(walker.NotYetCrawled) {
		return ""
	}
	return t.Format(timeFormat)
}

func fuuidFunc(u gocql.UUID) string {
	if u == zeroUUID {
		return ""
	}
	return u.String()
}

// Render is the global render.Render object used by all controllers
var Render *render.Render

// BuildRender builds Render
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

//
// S E S S I O N S
//
const DefaultPageWindowLength = 15

// PageWindowLengthChoices lists the window length's listed on /list and /links page
var PageWindowLengthChoices = []int{10, 15, 25, 35, 50, 75, 100, 150, 250}
var sessionManager = sessions.NewCookieStore([]byte("01234567890123456789012345678901"))

// Session object manages all controller sessions
type Session struct {
	req           *http.Request
	w             http.ResponseWriter
	sess          *sessions.Session
	lastSaveError error
}

// GetSession returns a session object
func GetSession(w http.ResponseWriter, req *http.Request) (*Session, error) {
	sess, err := sessionManager.Get(req, "walker")
	if err != nil {
		return nil, err
	}
	return &Session{req: req, w: w, sess: sess}, nil
}

func (sess *Session) save() {
	sess.lastSaveError = sess.sess.Save(sess.req, sess.w)
}

// ListPageWindowLength returns page length for /list page
func (sess *Session) ListPageWindowLength() int {
	val, valOk := sess.sess.Values["pwl"]
	if !valOk {
		return DefaultPageWindowLength
	}
	pwl, pwlOk := val.(int)
	if !pwlOk {
		return DefaultPageWindowLength
	}

	return pwl
}

func (sess *Session) SetListPageWindowLength(plen int) {
	sess.sess.Values["pwl"] = plen
	sess.save()
}

// LinksPageWindowLength returns the page length for /links page
func (sess *Session) LinksPageWindowLength() int {
	val, valOk := sess.sess.Values["lpwl"]
	if !valOk {
		return DefaultPageWindowLength
	}
	pwl, pwlOk := val.(int)
	if !pwlOk {
		return DefaultPageWindowLength
	}

	return pwl
}

func (sess *Session) SetLinksPageWindowLength(plen int) {
	sess.sess.Values["lpwl"] = plen
	sess.save()
}

func (sess *Session) AddInfoFlash(message string) {
	sess.sess.AddFlash("I" + message)
	sess.save()
}

func (sess *Session) AddErrorFlash(message string) {
	sess.sess.AddFlash("E" + message)
	sess.save()
}

func (sess *Session) Flashes() (infos []string, errors []string) {
	for _, flash := range sess.sess.Flashes() {
		s, sok := flash.(string)
		if !sok || len(s) < 1 {
			continue
		}

		if s[0] == 'I' {
			infos = append(infos, s[1:])
		} else if s[0] == 'E' {
			errors = append(errors, s[1:])
		}
	}

	sess.save()
	return
}
