// handlers
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/elgs/gorest2"
	"github.com/elgs/gosqljson"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func init() {

	gorest2.RegisterHandler("/auth/github", func(w http.ResponseWriter, r *http.Request) {
		type AccessTokenParams struct {
			Code        string `json:"code"`
			ClientId    string `json:"clientId"`
			RedirectUri string `json:"redirectUri"`
		}

		clientSecret := grConfig["secret_github"].(string)
		var accessTokenParams AccessTokenParams
		accessTokenDecoder := json.NewDecoder(r.Body)
		err := accessTokenDecoder.Decode(&accessTokenParams)
		if err != nil {
			fmt.Println(err)
		}
		var accessTokenUrl string = "https://github.com/login/oauth/access_token"
		var accessTokenQs = fmt.Sprintf("client_id=%v&redirect_uri=%v&client_secret=%v&code=%v",
			accessTokenParams.ClientId, accessTokenParams.RedirectUri, clientSecret, accessTokenParams.Code)
		accessTokenResp, err := http.Get(accessTokenUrl + "?" + accessTokenQs)
		if err != nil {
			fmt.Println(err)
		}
		defer accessTokenResp.Body.Close()

		var userApiUrl string = "https://api.github.com/user"
		accessTokenBody, err := ioutil.ReadAll(accessTokenResp.Body)
		if err != nil {
			fmt.Println(err)
		}
		apiResp, err := http.Get(userApiUrl + "?" + string(accessTokenBody))
		if err != nil {
			fmt.Println(err)
		}
		defer apiResp.Body.Close()
		body, err := ioutil.ReadAll(apiResp.Body)
		if err != nil {
			fmt.Println(err)
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(body, &m)
		if err != nil {
			fmt.Println(err)
		}

		for k, v := range m {
			fmt.Println(k, ": ", v)
		}

		tokenMap := map[string]interface{}{
			"name":    m["name"],
			"email":   m["email"],
			"picture": m["avatar_url"],
			"type":    "github",
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		err = FindOrCreateUser(db, tokenMap)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Email already used."))
			return
		}

		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))
	})

	gorest2.RegisterHandler("/auth/facebook", func(w http.ResponseWriter, r *http.Request) {
		type AccessTokenParams struct {
			Code        string `json:"code"`
			ClientId    string `json:"clientId"`
			RedirectUri string `json:"redirectUri"`
		}

		type ApiParams struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
			ExpiresIn   int64  `json:"expires_in"`
		}

		clientSecret := grConfig["secret_facebook"].(string)
		var accessTokenParams AccessTokenParams
		accessTokenDecoder := json.NewDecoder(r.Body)
		err := accessTokenDecoder.Decode(&accessTokenParams)
		if err != nil {
			fmt.Println(err)
		}
		var accessTokenUrl string = "https://graph.facebook.com/v2.3/oauth/access_token"
		var accessTokenQs = fmt.Sprintf("client_id=%v&redirect_uri=%v&client_secret=%v&code=%v",
			accessTokenParams.ClientId, accessTokenParams.RedirectUri, clientSecret, accessTokenParams.Code)
		accessTokenResp, err := http.Get(accessTokenUrl + "?" + accessTokenQs)
		if err != nil {
			fmt.Println(err)
		}
		defer accessTokenResp.Body.Close()

		var apiParams ApiParams
		apiDecoder := json.NewDecoder(accessTokenResp.Body)
		err = apiDecoder.Decode(&apiParams)
		if err != nil {
			fmt.Println(err)
		}
		var userApiUrl string = "https://graph.facebook.com/v2.3/me"
		var apiQs = fmt.Sprintf("access_token=%v", apiParams.AccessToken)
		apiResp, err := http.Get(userApiUrl + "?" + apiQs)
		if err != nil {
			fmt.Println(err)
		}
		defer apiResp.Body.Close()
		body, err := ioutil.ReadAll(apiResp.Body)
		if err != nil {
			fmt.Println(err)
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(body, &m)
		if err != nil {
			fmt.Println(err)
		}

		for k, v := range m {
			fmt.Println(k, ": ", v)
		}

		tokenMap := map[string]interface{}{
			"name":  m["name"],
			"email": m["email"],
			"type":  "facebook",
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		// ignore the error if user existed.
		err = FindOrCreateUser(db, tokenMap)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Email already used."))
			return
		}

		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))

	})

	gorest2.RegisterHandler("/auth/google", func(w http.ResponseWriter, r *http.Request) {
		type AccessTokenParams struct {
			Code        string `json:"code"`
			ClientId    string `json:"clientId"`
			RedirectUri string `json:"redirectUri"`
			GrantType   string `json:"authorization_code"`
		}
		type ApiParams struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
			ExpiresIn   int64  `json:"expires_in"`
			IdToken     string `json:"id_token"`
		}

		clientSecret := grConfig["secret_google"].(string)
		var accessTokenParams AccessTokenParams
		accessTokenDecoder := json.NewDecoder(r.Body)
		err := accessTokenDecoder.Decode(&accessTokenParams)
		if err != nil {
			fmt.Println(err)
		}
		var accessTokenUrl string = "https://accounts.google.com/o/oauth2/token"
		accessTokenResp, err := http.PostForm(accessTokenUrl,
			url.Values{"code": {accessTokenParams.Code}, "client_id": {accessTokenParams.ClientId},
				"client_secret": {clientSecret}, "redirect_uri": {accessTokenParams.RedirectUri},
				"grant_type": {"authorization_code"}})
		if err != nil {
			fmt.Println(err)
		}
		defer accessTokenResp.Body.Close()

		var apiParams ApiParams
		apiDecoder := json.NewDecoder(accessTokenResp.Body)
		err = apiDecoder.Decode(&apiParams)
		if err != nil {
			fmt.Println(err)
		}
		var userApiUrl string = "https://www.googleapis.com/plus/v1/people/me/openIdConnect"
		apiClient := &http.Client{}
		req, err := http.NewRequest("GET", userApiUrl, nil)
		if err != nil {
			fmt.Println(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiParams.AccessToken)
		apiResp, err := apiClient.Do(req)
		if err != nil {
			fmt.Println(err)
		}

		defer apiResp.Body.Close()
		body, err := ioutil.ReadAll(apiResp.Body)
		if err != nil {
			fmt.Println(err)
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(body, &m)
		if err != nil {
			fmt.Println(err)
		}

		for k, v := range m {
			fmt.Println(k, ": ", v)
		}

		tokenMap := map[string]interface{}{
			"name":    m["name"],
			"email":   m["email"],
			"picture": m["picture"],
			"type":    "google",
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		// ignore the error if user existed.
		err = FindOrCreateUser(db, tokenMap)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Email already used."))
			return
		}

		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))

	})

	gorest2.RegisterHandler("/auth/live", func(w http.ResponseWriter, r *http.Request) {
		type AccessTokenParams struct {
			Code        string `json:"code"`
			ClientId    string `json:"clientId"`
			RedirectUri string `json:"redirectUri"`
			GrantType   string `json:"authorization_code"`
		}
		type ApiParams struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
			ExpiresIn   int64  `json:"expires_in"`
			Scope       string `json:"scope"`
			UserId      string `json:"user_id"`
		}

		clientSecret := grConfig["secret_live"].(string)
		var accessTokenParams AccessTokenParams
		accessTokenDecoder := json.NewDecoder(r.Body)
		err := accessTokenDecoder.Decode(&accessTokenParams)
		if err != nil {
			fmt.Println(err)
		}
		var accessTokenUrl string = "https://login.live.com/oauth20_token.srf"
		accessTokenResp, err := http.PostForm(accessTokenUrl,
			url.Values{"code": {accessTokenParams.Code}, "client_id": {accessTokenParams.ClientId},
				"client_secret": {clientSecret}, "redirect_uri": {accessTokenParams.RedirectUri},
				"grant_type": {"authorization_code"}})
		if err != nil {
			fmt.Println(err)
		}
		defer accessTokenResp.Body.Close()

		var apiParams ApiParams
		apiDecoder := json.NewDecoder(accessTokenResp.Body)
		err = apiDecoder.Decode(&apiParams)
		if err != nil {
			fmt.Println(err)
		}
		var userApiUrl string = "https://apis.live.net/v5.0/me"
		var apiQs = fmt.Sprintf("access_token=%v", apiParams.AccessToken)
		apiResp, err := http.Get(userApiUrl + "?" + apiQs)
		if err != nil {
			fmt.Println(err)
		}
		defer apiResp.Body.Close()
		body, err := ioutil.ReadAll(apiResp.Body)
		if err != nil {
			fmt.Println(err)
		}
		m := make(map[string]interface{})
		err = json.Unmarshal(body, &m)
		if err != nil {
			fmt.Println(err)
		}

		for k, v := range m {
			fmt.Println(k, ": ", v)
		}

		var email interface{}
		if emails, ok := m["emails"].(map[string]interface{}); ok {
			email = emails["account"]
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Email not found."))
			return
		}
		tokenMap := map[string]interface{}{
			"name":  m["name"],
			"email": email,
			"type":  "live",
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		// ignore the error if user existed.
		err = FindOrCreateUser(db, tokenMap)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Email already used."))
			return
		}

		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))

	})

	gorest2.RegisterHandler("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		type LoginParams struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		var loginParams LoginParams
		loginDecoder := json.NewDecoder(r.Body)
		err := loginDecoder.Decode(&loginParams)
		if err != nil {
			fmt.Println(err)
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		query := `SELECT user.*, IFNULL(roles.ROLES,'') AS ROLES FROM user LEFT OUTER JOIN (
			SELECT USER_EMAIL,GROUP_CONCAT(ROLE_NAME) AS ROLES FROM user_role GROUP BY USER_EMAIL
			) AS roles ON user.EMAIL=roles.USER_EMAIL WHERE user.EMAIL=? AND user.PASSWORD=? AND user.TYPE=?`
		data, err := gosqljson.QueryDbToMap(db, "", query,
			loginParams.Email, loginParams.Password, "signup")
		if err != nil {
			fmt.Println(err)
		}
		if data == nil || len(data) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Wrong email and/or password"))
			return
		}

		tokenMap := map[string]interface{}{
			"id":       data[0]["ID"],
			"tokenKey": data[0]["TOKEN_KEY"],
			"name":     data[0]["USERNAME"],
			"email":    loginParams.Email,
			"type":     "login",
			"roles":    data[0]["ROLES"],
		}
		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))
	})

	gorest2.RegisterHandler("/auth/signup", func(w http.ResponseWriter, r *http.Request) {
		var signupParams SignupParams
		loginDecoder := json.NewDecoder(r.Body)
		err := loginDecoder.Decode(&signupParams)
		if err != nil {
			fmt.Println(err)
		}

		projectId := r.Header.Get("app_id")
		if projectId == "" {
			projectId = "default"
		}
		dbo := gorest2.GetDbo(projectId)
		db, err := dbo.GetConn()
		if err != nil {
			fmt.Println(err)
		}

		tokenMap := map[string]interface{}{
			"name":         signupParams.Name,
			"email":        signupParams.Email,
			"type":         "signup",
			"phone_number": signupParams.PhoneNumber,
			"password":     signupParams.Password,
			"status":       "0",
		}

		err = CreateUser(db, tokenMap)
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(err.Error()))
			return
		}

		token, err := CreateJwtToken(tokenMap)
		if err != nil {
			fmt.Println(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"token":"%v"}`, token)))
	})
}

type SignupParams struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	Password    string `json:"password"`
	PhoneNumber string `json:"phone_number"`
}

func FindOrCreateUser(db *sql.DB, userData map[string]interface{}) error {
	query := `SELECT user.*, IFNULL(roles.ROLES,'') AS ROLES FROM user LEFT OUTER JOIN (
			SELECT USER_EMAIL,GROUP_CONCAT(ROLE_NAME) AS ROLES FROM user_role GROUP BY USER_EMAIL
			) AS roles ON user.EMAIL=roles.USER_EMAIL WHERE user.EMAIL=? AND user.TYPE=?`
	data, err := gosqljson.QueryDbToMap(db, "", query,
		userData["email"], userData["type"])
	if err != nil || data == nil || len(data) == 0 {
		return CreateUser(db, userData)
	} else {
		userData["id"] = data[0]["ID"]
		userData["tokenKey"] = data[0]["TOKEN_KEY"]
		userData["roles"] = data[0]["ROLES"]
		return nil
	}
}

func CreateUser(db *sql.DB, userData map[string]interface{}) error {
	password := userData["password"]
	if password == nil {
		password = ""
	}
	picture := userData["picture"]
	if picture == nil {
		picture = ""
	}
	phoneNumber := userData["phone_number"]
	if phoneNumber == nil {
		phoneNumber = ""
	}
	status := userData["status"]
	if status == nil {
		status = "0"
	}
	user := map[string]interface{}{
		"ID":           strings.Replace(uuid.NewV4().String(), "-", "", -1),
		"TYPE":         userData["type"],
		"TOKEN_KEY":    strings.Replace(uuid.NewV4().String(), "-", "", -1),
		"STATUS":       status,
		"USERNAME":     userData["name"],
		"EMAIL":        userData["email"],
		"PHONE_NUMBER": phoneNumber,
		"PICTURE_URL":  picture,
		"PASSWORD":     password,
		"TMP_KEY":      strings.Replace(uuid.NewV4().String(), "-", "", -1),
		"LAST_LOGIN":   time.Now(),
		"CREATOR_ID":   "system",
		"CREATOR_CODE": "system",
		"CREATE_TIME":  time.Now(),
		"UPDATER_ID":   "system",
		"UPDATER_CODE": "system",
		"UPDATE_TIME":  time.Now(),
	}
	rowsAffected, err := DbInsert(db, "user", user)

	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return errors.New("Failed to create account, invalid username/password, or user already existed, please try another username and try again.")
	}

	userData["id"] = user["ID"]
	userData["tokenKey"] = user["TOKEN_KEY"]
	return nil
}

func CreateJwtToken(m map[string]interface{}) (string, error) {
	token := jwt.New(jwt.SigningMethodHS256)
	for k, v := range m {
		token.Claims[k] = v
	}
	token.Claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	return token.SignedString([]byte("netdata.io"))
}
