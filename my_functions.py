@outputSchema('(subject:chararray,predicate:chararray,p_object:chararray)')
def clear_data(subject, predicate, p_object):
	subject = subject[1:-1].split('/')[-1]
	predicate = predicate[1:-1].split('/')[-1]
	if '/' in p_object:
		p_object = p_object[1:-1].split('/')[-1]
	if 'type.object.type' in predicate:
		predicate = ''
		p_object = p_object.replace('.', ' ')
	else:
		predicate = predicate.replace('.', ' ')
	return subject, predicate, p_object
	
@outputSchema('(subject:chararray,predicate:chararray,p_object:chararray)')
def clear_object(subject, predicate, p_object):
	if '"' in p_object:
		quotation = [pos for pos, char in enumerate(p_object) if char == '"']
		p_object = p_object[quotation[0] + 1:quotation[1]]
	return subject, predicate, p_object
	
@outputSchema('(subject:chararray,object:chararray)')
def clear_names(group, joined_data):
	subject = ['', '']
	for item in joined_data:
		if '@en' in item[1]:
			subject = [item[1], 'en']
		elif '@de' in item[1] and subject[1] != 'en':
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