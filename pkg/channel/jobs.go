/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package channel

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ValidateJobRoutePath validates an optional path suffix used to deliver a job
// to an app. The path is relative to the /job/ endpoint and must not contain
// URL control components or ambiguous path segments.
func ValidateJobRoutePath(routePath string) error {
	if routePath == "" {
		return nil
	}

	if !utf8.ValidString(routePath) || strings.HasPrefix(routePath, "/") || strings.HasSuffix(routePath, "/") {
		return errors.New("override_route_path must be a valid relative HTTP path without leading or trailing slashes")
	}

	for _, segment := range strings.Split(routePath, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("override_route_path must not contain empty, \".\", or \"..\" path segments")
		}

		for _, r := range segment {
			if unicode.IsControl(r) || r == '\\' || r == '?' || r == '#' || r == '%' {
				return errors.New("override_route_path contains a character that is not allowed in an HTTP path segment")
			}
		}
	}

	return nil
}
