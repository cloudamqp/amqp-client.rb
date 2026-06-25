# frozen_string_literal: true

require "rdoc"
require "rdoc/generator/aliki"

# Project-specific hooks for the RDoc Aliki theme.
#
# RDoc opens only the top-level class tree node by default. AMQP::Client is the
# public namespace users browse most often, so opening it in the generated HTML
# makes the left-side navigation more discoverable. We do it in the generator
# instead of CSS or JavaScript so the native <details> state, chevron styling,
# and accessibility state all agree.
module RDocLavinMQTheme
  DEFAULT_OPEN_CLASS_INDEX_NAMES = ["AMQP::Client"].freeze

  # Match RDoc's inherited method signature and keep this close to the upstream
  # implementation, only changing which parent class entries get `open`.
  # rubocop:disable Style/OptionalBooleanParameter
  def traverse_classes(klasses, grouped_classes, rel_prefix, solo = false)
    content = +'<ul class="link-list nav-list">'

    klasses.each do |index_klass|
      children = grouped_classes[index_klass.full_name]

      if children
        open = solo || default_open_class_index?(index_klass)
        content << class_index_parent(index_klass, rel_prefix, open)
        content << traverse_classes(children, grouped_classes, rel_prefix)
        content << "</details></li>"
        solo = false
      elsif index_klass.display?
        content << %(<li>#{generate_class_link(index_klass, rel_prefix)}</li>)
      end
    end

    "#{content}</ul>"
  end
  # rubocop:enable Style/OptionalBooleanParameter

  private

  def class_index_parent(index_klass, rel_prefix, open)
    open_attr = open ? " open" : ""
    link = generate_class_link(index_klass, rel_prefix)

    %(<li><details#{open_attr}><summary>#{link}</summary>)
  end

  def default_open_class_index?(index_klass)
    DEFAULT_OPEN_CLASS_INDEX_NAMES.include?(index_klass.full_name)
  end
end

RDoc::Generator::Aliki.prepend(RDocLavinMQTheme)
