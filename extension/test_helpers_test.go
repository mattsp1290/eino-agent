package extension

import (
	"errors"
)

type testPayload struct {
	Protected string
	Values    []string
}

var (
	testNotice = NewNotification(Contract{ID: "test/notice", Version: "1"}, clonePayload)
	testAround = NewRequiredAround(Contract{ID: "test/around", Version: "1"}, clonePayload, func(output string) error {
		if output == "" {
			return errors.New("empty output")
		}
		return nil
	})
)

func clonePayload(input testPayload) (testPayload, error) {
	input.Values = append([]string(nil), input.Values...)
	return input, nil
}

func testComponent(id string) Component {
	return Component{InstanceID: id, Artifact: Artifact{Name: "tests", Version: "1", Hash: "artifact-hash", ConfigHash: "config-hash", SourceKind: SourceNative}}
}

func spec(_ string, id string, order int, scope Scope) Registration {
	return Registration{ID: id, Order: order, Scope: scope}
}
