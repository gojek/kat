package io

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMirrorStatus_CreateSuccess(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := true
	isDryRun := false

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, nil)

	assert.Equal(t, []string{"topic-1", "Create", "config-1", "10", "20", "Success", ""}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_CreateFailure(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := true
	isDryRun := false
	err := errors.New("error")

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, err)

	assert.Equal(t, []string{"topic-1", "Create", "config-1", "10", "20", "Failure", "error"}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_CreateDryRun(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := true
	isDryRun := true

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, nil)

	assert.Equal(t, []string{"topic-1", "Create", "config-1", "10", "20", "DryRun", ""}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_UpdateSuccess(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := false
	isDryRun := false

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, nil)

	assert.Equal(t, []string{"topic-1", "Update", "config-1", "10", "20", "Success", ""}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_UpdateFailure(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := false
	isDryRun := false
	err := errors.New("error")

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, err)

	assert.Equal(t, []string{"topic-1", "Update", "config-1", "10", "20", "Failure", "error"}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_UpdateDryRun(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := false
	isDryRun := true

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, nil)

	assert.Equal(t, []string{"topic-1", "Update", "config-1", "10", "20", "DryRun", ""}, mirrorStatus.FieldValues())
}

func TestMirrorStatus_Headers(t *testing.T) {
	topic := "topic-1"
	config := "config-1"
	oldNumOfPartitions := int32(10)
	newNumOfPartitions := int32(20)
	isCreate := false
	isDryRun := true

	mirrorStatus := MirrorStatus(topic, config, oldNumOfPartitions, newNumOfPartitions, isCreate, isDryRun, nil)

	assert.Equal(t, []string{"Topic", "Action", "Configs", "OldPartitionCount", "NewPartitionCount", "Status", "Reason"}, mirrorStatus.Headers())
}
