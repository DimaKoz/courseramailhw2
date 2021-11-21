package main

import (
	"testing"
)

func TestOriginalSigner(t *testing.T) {

	testExpected := "1173136728138862632818075107442090076184424490584241521304_1696913515191343735512658979631549563179965036907783101867_27225454331033649287118297354036464389062965355426795162684_29568666068035183841425683795340791879727309630931025356555_3994492081516972096677631278379039212655368881548151736_4958044192186797981418233587017209679042592862002427381542_4958044192186797981418233587017209679042592862002427381542"
	testResult := "NOT_SET"

	inputData := []int{0, 1, 1, 2, 3, 5, 8}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			data, ok := dataRaw.(string)
			if !ok {
				t.Error("cant convert result data to string")
			}
			testResult = data
		}),
	}



	ExecutePipeline(hashSignJobs...)


	if testExpected != testResult {
		t.Errorf("results not match\nGot: %v\nExpected: %v", testResult, testExpected)
	}


}

