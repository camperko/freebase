@outputSchema('(subject:chararray,object:chararray)')
def clear_data(subject, p_object):
	quotation = [pos for pos, char in enumerate(p_object) if char == '"']
	p_object = p_object[quotation[0] + 1:quotation[1]]
	return subject, p_object
	
@outputSchema('(subject:chararray,object:chararray)')
def filter_data(group, object_data):
	output = ['', '']
	for item in object_data:
		if '@en' in item[2]:
			output = [item[2], 'en']
		elif '@sk' in item[2] and output[1] != 'en':
			output = [item[2], 'sk']
		elif '@cs' in item[2] and output[1] != 'en' and output[1] != 'sk':
			output = [item[2], 'cs']
		elif '@de' in item[2] and output[1] != 'en' and output[1] != 'sk' and output[1] != 'cs':
			output = [item[2], 'de']
		elif output[1] == '':
			output = [item[2], 'xx']
	quotation = [pos for pos, char in enumerate(output[0]) if char == '@']
	if len(quotation) >= 1:
		output = output[0][1:quotation[len(quotation)-1]-1]
	else:
		quotation = [pos for pos, char in enumerate(output[0]) if char == '"']
		output = output[0][quotation[0] + 1:quotation[len(quotation)-1]]
	return group, output
			

@outputSchema('(subject:chararray,object:chararray)')
def clear_names(group, joined_data):
	subject = ['', '']
	for item in joined_data:
		if '@en' in item[1]:
			subject = [item[1], 'en']
		elif '@sk' in item[1] and subject[1] != 'en':
			subject = [item[1], 'sk']
		elif '@cs' in item[1] and subject[1] != 'en' and subject[1] != 'sk':
			subject = [item[1], 'cs']
		elif '@de' in item[1] and subject[1] != 'en' and subject[1] != 'sk' and subject[1] != 'cs':
			subject = [item[1], 'de']
		elif subject[1] == '':
			subject = [item[1], 'xx']	
	quotation = [pos for pos, char in enumerate(subject[0]) if char == '@']
	if len(quotation) >= 1:
		subject = subject[0][1:quotation[len(quotation)-1]-1]
	else:
		quotation = [pos for pos, char in enumerate(subject[0]) if char == '"']
		subject = subject[0][quotation[0] + 1:quotation[len(quotation)-1]]
	return group, subject
	
@outputSchema('(subject:chararray,object:chararray)')
def extract_type(subject, p_object):
	p_object = p_object[1:-1].split('/')[-1].replace('.', ' ')
	return subject, p_object
	
@outputSchema('(subject:chararray,merged_data:bag{tuple_0:(object:chararray)})')
def join_bags(subject1, bag1, subject2, bag2):
    subject = subject1
    if subject is None:
        subject = subject2
    if bag1 is None:
        return subject, bag2
    elif bag2 is None:
        return subject, bag1
    for item in bag2:
        if item not in bag1:
            bag1.append(item)
    return subject, bag1;